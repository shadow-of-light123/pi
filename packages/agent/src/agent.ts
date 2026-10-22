/**
 * Agent 的有状态门面：在 `agent-loop.ts` 的无状态循环之上维护 transcript 与工具集，
 * 并把运行生命周期事件串行分发给订阅者。
 *
 * 分层职责：
 * - `agent-loop.ts`：模型请求 -> 工具执行 -> 结果回填的循环，本身不保存状态，也不关心
 *   事件最终去了哪里。
 * - `agent.ts`（本文件）：保存 `state.messages`/`state.tools`，负责 `prompt`/`continue`/
 *   `reset`/`abort` 的互斥与生命周期，并把 steering/followUp 队列接到循环的轮询点上。
 * - `types.ts`：两边共享的契约（`AgentEvent`、`AgentLoopConfig`、`StreamFn` 等）。
 *
 * 三条关键约定：
 * 1. 事件串行：`processEvents` 先把事件归并进 state，再按订阅顺序逐个 await listener，
 *    保证 UI/持久化看到的事件顺序与 state 一致；一次运行要等 `agent_end` 的监听器都结束
 *    才被判定为 idle。
 * 2. 模型边界只有一处：内部一律用可扩展的 `AgentMessage`，真正请求前才通过
 *    `convertToLlm` 转成 provider 认识的 `Message[]`（见 `createLoopConfig`）。
 * 3. 运行失败不向调用方 reject：错误被合成为一条 `stopReason` 为 "error"/"aborted" 的
 *    assistant 消息，走完整的事件生命周期后由 `agent_end` 收尾（见 `handleRunFailure`）。
 */

import {
	type ClientIdentity,
	// 把 systemPrompt/tools 折叠成 transcript 开头的一条 system 消息；两者都为空时返回 undefined。
	createInitialSystemMessage,
	// 从 transcript 的所有 system 消息里回放出“当前生效”的 prompt 与工具声明。
	getCurrentSystemMessage,
	// 回放 system 消息的渲染文本；`state.systemPrompt` 就是它的只读视图。
	getCurrentSystemPrompt,
	type ImageContent,
	type Message,
	type Model,
	type SimpleStreamOptions,
	type TextContent,
	type ThinkingBudgets,
	type Transport,
	// 去掉 execute/render 等运行时字段，只保留模型可见的 name/description/parameters。
	toToolDeclaration,
} from "@earendil-works/pi-ai";
// 直接 await 到循环结束的入口：便于把同一次运行纳入 activeRun 生命周期与统一错误收尾。
import { runAgentLoop, runAgentLoopContinue } from "./agent-loop.ts";
// 宿主通过 setDefaultStreamFn 注入默认模型运行时，agent-core 因此不依赖具体 provider 目录。
import { getDefaultStreamFn } from "./stream-fn.ts";
import type {
	AfterToolCallContext,
	AfterToolCallResult,
	AgentContext,
	AgentEvent,
	AgentLoopConfig,
	AgentLoopTurnUpdate,
	AgentMessage,
	AgentState,
	AgentTool,
	BeforeToolCallContext,
	BeforeToolCallResult,
	FinishTurn,
	PrepareNextTurnContext,
	PrepareRequest,
	QueueMode,
	StreamFn,
	ToolExecutionMode,
} from "./types.ts";

// 重新导出：只想用 agent-core 的宿主不必再单独 import types.ts。
export type { QueueMode } from "./types.ts";

/**
 * 默认的模型边界转换：只保留 provider 认识的四种角色，丢掉自定义消息（如 UI 通知）。
 *
 * 这是 `AgentMessage` 与 `Message` 的分界：内部 transcript 可以更丰富，但发给模型的请求
 * 不能包含未知角色。需要压缩/改写上下文时请在 `transformContext` 里做，而不是放宽这里，
 * 否则自定义消息会被直接发给 provider。
 */
function defaultConvertToLlm(messages: AgentMessage[]): Message[] {
	return messages.filter(
		(message) =>
			message.role === "system" ||
			message.role === "user" ||
			message.role === "assistant" ||
			message.role === "toolResult",
	);
}

/** 运行失败时合成 assistant 消息用的零用量：保持消息形状完整，下游可安全读取 usage/cost。 */
const EMPTY_USAGE = {
	input: 0,
	output: 0,
	cacheRead: 0,
	cacheWrite: 0,
	totalTokens: 0,
	cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};

/**
 * 没有显式配置模型时的占位模型。
 *
 * Agent 可能先于模型解析被构造（例如 UI 先渲染、测试只关心事件流），此时 `state.model`
 * 仍必须是非空对象。占位值让 Agent 能安全构造和读取；真正发请求前宿主应设置真实模型。
 * `satisfies Model<any>` 只做类型校验，不产生运行时代码。
 */
const DEFAULT_MODEL = {
	id: "unknown",
	name: "unknown",
	api: "unknown",
	provider: "unknown",
	baseUrl: "",
	reasoning: false,
	input: [],
	cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
	contextWindow: 0,
	maxTokens: 0,
} satisfies Model<any>;

/**
 * 内部可变状态：对外的 `AgentState` 把运行期字段声明为只读（订阅者不应直接改），
 * 而 Agent 自己要在事件处理里写它们。这里用 `Omit` 去掉只读约束后重新声明，
 * 明确这 4 个字段由运行时独占，外部即使拿到 `state` 也不应整体替换。
 */
type MutableAgentState = Omit<AgentState, "isStreaming" | "streamingMessage" | "pendingToolCalls" | "errorMessage"> & {
	isStreaming: boolean;
	streamingMessage?: AgentMessage;
	pendingToolCalls: Set<string>;
	errorMessage?: string;
};

/**
 * {@link Agent} 的初始状态：`systemPrompt` 和 `tools` 会被折叠成 transcript 开头的一条
 * system 消息，除非 `messages` 已经以 system 消息开头。
 *
 * 运行期字段（`isStreaming`、`streamingMessage`、`pendingToolCalls`、`errorMessage`）由 Agent
 * 独占，因此不在这里开放：初始状态只描述对话内容与静态配置。
 */
export type AgentInitialState = Partial<
	Omit<AgentState, "pendingToolCalls" | "isStreaming" | "streamingMessage" | "errorMessage">
>;

/**
 * 把调用方提供的初始状态整理成内部可变状态。
 *
 * 几处细节：
 * - `tools`/`messages` 先复制顶层数组：调用方持有自己的数组，之后 push/splice 不会互相影响。
 * - `systemPrompt` 与 `tools` 会固化到首条 system 消息里，且工具经 `toToolDeclaration` 去掉
 *   execute/render 等运行时字段——transcript 只保存模型可见的声明，避免把闭包/句柄写进
 *   会话历史（历史会被持久化序列化）。
 * - `state.systemPrompt` 不是存下来的字符串，而是每次从 transcript 回放得到；因此后续
 *   system 消息里的增量修改也能反映出来。
 * - 占位模型与 thinkingLevel "off" 让 Agent 在没有模型时也能安全构造。
 */
function createMutableAgentState(initialState?: AgentInitialState): MutableAgentState {
	let tools = initialState?.tools?.slice() ?? [];
	let messages = initialState?.messages?.slice() ?? [];
	const initialMessage = createInitialSystemMessage(initialState?.systemPrompt, tools.map(toToolDeclaration));
	// 只有 messages 不是以 system 消息开头时才补基线，避免和调用方自带的 system 消息重复。
	if (messages[0]?.role !== "system" && initialMessage) messages.unshift(initialMessage);

	return {
		get systemPrompt() {
			return getCurrentSystemPrompt(messages);
		},
		model: initialState?.model ?? DEFAULT_MODEL,
		thinkingLevel: initialState?.thinkingLevel ?? "off",
		get tools() {
			return tools;
		},
		// 赋值时复制顶层数组：state.tools 是只读视图，但宿主替换工具集不应与内部数组共享引用。
		set tools(nextTools: AgentTool<any>[]) {
			tools = nextTools.slice();
		},
		get messages() {
			return messages;
		},
		// 同上：整体替换 transcript 时复制一层，内部继续用自己持有的数组。
		set messages(nextMessages: AgentMessage[]) {
			messages = nextMessages.slice();
		},
		isStreaming: false,
		streamingMessage: undefined,
		pendingToolCalls: new Set<string>(),
		errorMessage: undefined,
	};
}

/**
 * 构造 {@link Agent} 的参数。
 *
 * 除 `streamFn` 外都有默认值；未提供时的默认策略：占位模型、思考级别 "off"、
 * 工具并行执行、队列逐条消费、传输方式 "auto"。
 */
export interface AgentOptions {
	initialState?: AgentInitialState;
	/** 内部消息到 provider 消息的边界转换；默认只保留 provider 认识的四种角色。 */
	convertToLlm?: (messages: AgentMessage[]) => Message[] | Promise<Message[]>;
	/** 每次请求前的上下文改写钩子（压缩、裁剪、注入检索结果等），在 convertToLlm 之前作用于 `AgentMessage[]`。 */
	transformContext?: (messages: AgentMessage[], signal?: AbortSignal) => Promise<AgentMessage[]>;
	/** 真正发请求的函数；测试可注入假流，生产环境通常由宿主通过 setDefaultStreamFn 提供默认实现。 */
	streamFn: StreamFn;
	/** 按需解析 API key；由 provider 适配层在请求前调用。 */
	getApiKey?: (provider: string) => Promise<string | undefined> | string | undefined;
	/** 请求体发出前的观察/修改钩子。 */
	onPayload?: SimpleStreamOptions["onPayload"];
	/** 收到 HTTP 响应、流式内容开始前的钩子。 */
	onResponse?: SimpleStreamOptions["onResponse"];
	/** provider 原始流事件的观察钩子（调试/指标用）。 */
	onProviderStreamEvent?: SimpleStreamOptions["onProviderStreamEvent"];
	/** 工具执行前调用；返回结果可阻止本次调用（block），入参含工具调用与当前上下文。 */
	beforeToolCall?: (context: BeforeToolCallContext, signal?: AbortSignal) => Promise<BeforeToolCallResult | undefined>;
	/** 工具执行后调用；返回结果可覆盖内容/错误标记，再作为工具结果发给模型。 */
	afterToolCall?: (context: AfterToolCallContext, signal?: AbortSignal) => Promise<AfterToolCallResult | undefined>;
	/** 每轮结束后决定循环结束还是继续（`action: "end" | "continue"`）；error/aborted 响应始终直接结束。 */
	finishTurn?: FinishTurn;
	/** 每次请求前调用，可替换 context/model/thinkingLevel。 */
	prepareRequest?: PrepareRequest;
	/** 旧版轮次准备钩子，只拿得到 signal；保留用于兼容已有宿主。 */
	prepareNextTurn?: (
		signal?: AbortSignal,
	) => Promise<AgentLoopTurnUpdate | undefined> | AgentLoopTurnUpdate | undefined;
	/** 新版轮次准备钩子，额外拿到上一轮快照；与 `prepareNextTurn` 同时存在时优先使用它。 */
	prepareNextTurnWithContext?: (
		context: PrepareNextTurnContext,
		signal?: AbortSignal,
	) => Promise<AgentLoopTurnUpdate | undefined> | AgentLoopTurnUpdate | undefined;
	/** steering 队列的消费策略："one-at-a-time" 每轮只注入一条（默认），"all" 一次全部注入。 */
	steeringMode?: QueueMode;
	/** follow-up 队列的消费策略，语义同上。 */
	followUpMode?: QueueMode;
	/** 会话标识，转发给需要它的 provider（如 prompt caching）。 */
	sessionId?: string;
	/** 按思考级别限制 token 预算，转发给 stream 函数。 */
	thinkingBudgets?: ThinkingBudgets;
	/** 首选传输方式；默认 "auto"，由 stream 函数按 provider 能力选择。 */
	transport?: Transport;
	/** provider 要求重试时允许的最大等待，避免被长时间挂起。 */
	maxRetryDelayMs?: number;
	/** 一条 assistant 消息里多个工具调用的执行方式（并行/串行）。 */
	toolExecution?: ToolExecutionMode;
	/**
	 * 请求身份，按字段合并到 `DEFAULT_AGENT_IDENTITY`（`{ attribution: PI_ATTRIBUTION }`），
	 * 因此只需设置自己关心的字段。传 `{ attribution: null }` 表示不发送归因请求头。
	 */
	identity?: ClientIdentity;
}

/**
 * steering/follow-up 共用的消息队列。
 *
 * 队列只存在于内存：`enqueue` 不产生事件，也不会立刻进入 `state.messages`；循环在合适的
 * 时机调用 `drain` 取走，取出的消息随后作为新的 user 消息进入下一轮请求。
 *
 * `mode` 决定一次 `drain` 取多少：
 * - "one-at-a-time"（默认）：只取最早一条，其余留到后续轮次，表现为模型逐条收到干预；
 * - "all"：一次全取，适合把多条消息合并成一次注入。
 */
class PendingMessageQueue {
	private messages: AgentMessage[] = [];
	public mode: QueueMode;

	constructor(mode: QueueMode) {
		this.mode = mode;
	}

	/** 入队；不通知订阅者，避免“消息已排队”与“消息真正进入 transcript”混淆。 */
	enqueue(message: AgentMessage): void {
		this.messages.push(message);
	}

	/** 队列是否还有消息（不消费）。 */
	hasItems(): boolean {
		return this.messages.length > 0;
	}

	/** 查看下一次 `drain` 会取走的消息，但不消费；"all" 返回副本，"one-at-a-time" 只返回队首。 */
	peek(): AgentMessage[] {
		if (this.mode === "all") return this.messages.slice();
		const first = this.messages[0];
		return first ? [first] : [];
	}

	/** 取出下一次要注入的消息：先 peek，再从队头切掉同样数量。 */
	drain(): AgentMessage[] {
		const drained = this.peek();
		this.messages = this.messages.slice(drained.length);
		return drained;
	}

	/** 清空队列（`reset()` 与宿主主动取消排队时使用）。 */
	clear(): void {
		this.messages = [];
	}
}

/**
 * 一次进行中的运行：`promise` 供 `waitForIdle` 等待，收尾时统一 resolve；
 * `abortController` 让 `abort()` 能同时中断流式请求与工具执行。
 */
type ActiveRun = {
	promise: Promise<void>;
	resolve: () => void;
	abortController: AbortController;
};

/**
 * 有状态的 Agent：低层 agent 循环之上的封装。
 *
 * 职责：
 * - 持有当前 transcript（`state.messages`）与工具集；
 * - 驱动 `runAgentLoop`/`runAgentLoopContinue`，把循环事件交给订阅者；
 * - 通过 `steer()`/`followUp()` 队列在循环的轮询点注入用户消息；
 * - 管理运行生命周期：同一时刻只允许一次运行，`abort()`/`waitForIdle()`/`reset()` 都围绕
 *   内部唯一的 `activeRun` 工作。
 */
export class Agent {
	private _state: MutableAgentState;
	// 按订阅顺序保存（Set 保持插入顺序）；事件分发时逐个 await，保证投递有序。
	private readonly listeners = new Set<(event: AgentEvent, signal: AbortSignal) => Promise<void> | void>();
	// 两种队列的区别在注入时机：steering 在当前轮结束后注入，follow-up 只在循环本来要结束时注入。
	private readonly steeringQueue: PendingMessageQueue;
	private readonly followUpQueue: PendingMessageQueue;

	// 下面这些钩子字段声明为 public：宿主可以在会话中途替换（如切换 provider、调整工具策略），
	// 每次运行开始时由 createLoopConfig 读取最新值组装进循环配置。
	public convertToLlm: (messages: AgentMessage[]) => Message[] | Promise<Message[]>;
	public transformContext?: (messages: AgentMessage[], signal?: AbortSignal) => Promise<AgentMessage[]>;
	public streamFunction: StreamFn;
	public getApiKey?: (provider: string) => Promise<string | undefined> | string | undefined;
	public onPayload?: SimpleStreamOptions["onPayload"];
	public onResponse?: SimpleStreamOptions["onResponse"];
	public onProviderStreamEvent?: SimpleStreamOptions["onProviderStreamEvent"];
	public beforeToolCall?: (
		context: BeforeToolCallContext,
		signal?: AbortSignal,
	) => Promise<BeforeToolCallResult | undefined>;
	public afterToolCall?: (
		context: AfterToolCallContext,
		signal?: AbortSignal,
	) => Promise<AfterToolCallResult | undefined>;
	public finishTurn?: FinishTurn;
	public prepareRequest?: PrepareRequest;
	public prepareNextTurn?: (
		signal?: AbortSignal,
	) => Promise<AgentLoopTurnUpdate | undefined> | AgentLoopTurnUpdate | undefined;
	public prepareNextTurnWithContext?: (
		context: PrepareNextTurnContext,
		signal?: AbortSignal,
	) => Promise<AgentLoopTurnUpdate | undefined> | AgentLoopTurnUpdate | undefined;
	private activeRun?: ActiveRun;
	/** 会话标识，转发给需要它的 provider；支持缓存感知的后端会用它区分缓存分区。 */
	public sessionId?: string;
	/** 按思考级别限制的 token 预算，转发给 stream 函数。 */
	public thinkingBudgets?: ThinkingBudgets;
	/** 首选传输方式，转发给 stream 函数。 */
	public transport: Transport;
	/** provider 要求重试时允许的最大等待时间。 */
	public maxRetryDelayMs?: number;
	/** 一条 assistant 消息里多个工具调用的执行策略。 */
	public toolExecution: ToolExecutionMode;
	/** 请求身份，转发给 stream 函数时按字段覆盖 `DEFAULT_AGENT_IDENTITY`。 */
	public identity?: ClientIdentity;

	constructor(options: AgentOptions) {
		// 旧编译产物可能不传 options 或 streamFn（尽管当前类型要求它们），这里做一次运行时兜底。
		const runtimeOptions: Partial<AgentOptions> = options ?? {};
		// 构造内部状态：首条 system 消息（prompt + 工具声明）在这里生成。
		this._state = createMutableAgentState(runtimeOptions.initialState);
		// 默认转换只保留 provider 认识的四种角色；需要自定义时由宿主传入。
		this.convertToLlm = runtimeOptions.convertToLlm ?? defaultConvertToLlm;
		this.transformContext = runtimeOptions.transformContext;
		// 不传 streamFn 时用宿主注入的默认实现；没有设置过则抛出可读错误（见 getDefaultStreamFn）。
		this.streamFunction = runtimeOptions.streamFn ?? getDefaultStreamFn();
		this.getApiKey = runtimeOptions.getApiKey;
		this.onPayload = runtimeOptions.onPayload;
		this.onResponse = runtimeOptions.onResponse;
		this.onProviderStreamEvent = runtimeOptions.onProviderStreamEvent;
		this.beforeToolCall = runtimeOptions.beforeToolCall;
		this.afterToolCall = runtimeOptions.afterToolCall;
		this.finishTurn = runtimeOptions.finishTurn;
		this.prepareRequest = runtimeOptions.prepareRequest;
		this.prepareNextTurn = runtimeOptions.prepareNextTurn;
		this.prepareNextTurnWithContext = runtimeOptions.prepareNextTurnWithContext;
		// 默认逐条消费：避免一次请求里塞入多条用户干预，破坏“一轮一条指令”的节奏。
		this.steeringQueue = new PendingMessageQueue(runtimeOptions.steeringMode ?? "one-at-a-time");
		this.followUpQueue = new PendingMessageQueue(runtimeOptions.followUpMode ?? "one-at-a-time");
		this.sessionId = runtimeOptions.sessionId;
		this.thinkingBudgets = runtimeOptions.thinkingBudgets;
		// "auto" 让 stream 函数按 provider 能力自行选择传输通道。
		this.transport = runtimeOptions.transport ?? "auto";
		this.maxRetryDelayMs = runtimeOptions.maxRetryDelayMs;
		// 默认并行执行同一轮的多个工具调用；需要确定性顺序时宿主可改为 "sequential"。
		this.toolExecution = runtimeOptions.toolExecution ?? "parallel";
		this.identity = runtimeOptions.identity;
	}

	/**
	 * 订阅 Agent 生命周期事件。
	 *
	 * - 订阅本身不会触发初始事件，只收到之后发生的 run 事件；
	 * - 监听器 promise 按订阅顺序依次 await，且属于当前运行的结算范围：`prompt()`/
	 *   `waitForIdle()` 要等 `agent_end` 的监听器都结束才 resolve；
	 * - 监听器拿得到当前运行的活动 abort signal；
	 * - 监听器抛错会中断本次运行并转入失败收尾，因此非致命错误应在监听器内部处理；
	 *   失败收尾本身也要经过同一批监听器，重复抛错会让异常最终传给调用方。
	 *
	 * 返回的函数用于退订，重复调用无副作用。
	 */
	subscribe(listener: (event: AgentEvent, signal: AbortSignal) => Promise<void> | void): () => void {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	}

	/**
	 * 当前的 Agent 状态。
	 *
	 * 返回内部状态对象本身（不是快照），订阅者总能读到最新值，但不要长期缓存该引用。
	 * 给 `state.tools`/`state.messages` 赋值时会复制顶层数组（见 createMutableAgentState），
	 * 数组内的消息对象仍是同一引用。
	 */
	get state(): AgentState {
		return this._state;
	}

	/** 控制 steering 队列的消费方式；可在运行中修改，只影响之后的 drain。 */
	set steeringMode(mode: QueueMode) {
		this.steeringQueue.mode = mode;
	}

	get steeringMode(): QueueMode {
		return this.steeringQueue.mode;
	}

	/** 控制 follow-up 队列的消费方式；可在运行中修改，只影响之后的 drain。 */
	set followUpMode(mode: QueueMode) {
		this.followUpQueue.mode = mode;
	}

	get followUpMode(): QueueMode {
		return this.followUpQueue.mode;
	}

	/**
	 * 排队一条消息，等当前 assistant 轮（含其工具执行）结束后注入。
	 *
	 * 这是“不打断、插话”的语义：消息不会立刻进入 transcript，也不能抢占正在执行的工具；
	 * 循环在轮次结束后轮询队列，把取到的消息作为新的 user 消息发进下一轮请求。
	 * Agent 空闲时调用它不会自动启动运行。
	 */
	steer(message: AgentMessage): void {
		this.steeringQueue.enqueue(message);
	}

	/**
	 * 排队一条消息，等 Agent 本来就要停下时再继续。
	 *
	 * 与 `steer()` 的区别是优先级更低：只有循环没有工具调用、也没有 steering 消息、
	 * 没有其他继续理由时才取 follow-up，适合“干完这件事再顺手做下一件”的任务。
	 */
	followUp(message: AgentMessage): void {
		this.followUpQueue.enqueue(message);
	}

	/** 清空 steering 队列（已注入的消息不受影响）。 */
	clearSteeringQueue(): void {
		this.steeringQueue.clear();
	}

	/** 清空 follow-up 队列（已注入的消息不受影响）。 */
	clearFollowUpQueue(): void {
		this.followUpQueue.clear();
	}

	/** 同时清空 steering 与 follow-up 队列。 */
	clearAllQueues(): void {
		this.clearSteeringQueue();
		this.clearFollowUpQueue();
	}

	/** 任一队列仍有待注入消息时返回 true。 */
	hasQueuedMessages(): boolean {
		return this.steeringQueue.hasItems() || this.followUpQueue.hasItems();
	}

	/**
	 * 预览下一次会注入哪些排队消息，但不消费。
	 *
	 * steering 优先于 follow-up，与循环内部的取用顺序一致（循环每轮结束先轮询 steering，
	 * 确定要停下前才取 follow-up）。
	 */
	peekQueuedMessages(): AgentMessage[] {
		const steering = this.steeringQueue.peek();
		return steering.length > 0 ? steering : this.followUpQueue.peek();
	}

	/** 当前运行的活动 abort signal；没有运行时为 undefined。 */
	get signal(): AbortSignal | undefined {
		return this.activeRun?.abortController.signal;
	}

	/**
	 * 中断当前运行（如果有）。
	 *
	 * 只取消这一次运行：流式请求与工具执行通过同一个 AbortSignal 停止，运行会以
	 * `stopReason: "aborted"` 走失败收尾并照常发出 `agent_end`。队列与 transcript 不受影响。
	 * 空闲时调用不抛错。
	 */
	abort(): void {
		this.activeRun?.abortController.abort();
	}

	/**
	 * 等待当前运行及其全部事件监听器结束。
	 *
	 * 在 `agent_end` 的监听器都 settle 之后才 resolve；Agent 空闲时立即 resolve。
	 */
	waitForIdle(): Promise<void> {
		return this.activeRun?.promise ?? Promise.resolve();
	}

	/**
	 * 清空对话状态与队列，只保留由 system 消息回放出的 prompt/工具基线。
	 *
	 * 运行中不允许 reset：循环仍可能往 transcript 追加事件，与重置后的状态交错会造成
	 * 状态错乱（有测试覆盖）。需要重置时先 `abort()`，再 `await waitForIdle()`。
	 */
	reset(): void {
		if (this.activeRun) {
			throw new Error("Agent is already processing. Wait for completion before resetting.");
		}

		// 只保留回放出的 system 基线（prompt 与工具声明），对话消息全部丢弃。
		const baseline = getCurrentSystemMessage(this._state.messages);
		this._state.messages = baseline ? [baseline] : [];
		this._state.isStreaming = false;
		this._state.streamingMessage = undefined;
		this._state.pendingToolCalls = new Set<string>();
		this._state.errorMessage = undefined;
		// 清掉队列，避免旧会话残留的排队消息被新会话消费。
		this.clearFollowUpQueue();
		this.clearSteeringQueue();
	}

	/**
	 * 启动一次新的运行，支持三种入参形式：文本（可带图片）、单条消息、一批消息。
	 *
	 * 运行期间不能再调用 prompt：需要追加输入时请用 `steer()`/`followUp()` 排队，或等待完成。
	 * 模型/工具错误不会向调用方 reject，而是以失败消息的形式进入 transcript 并触发 `agent_end`。
	 */
	async prompt(message: AgentMessage | AgentMessage[]): Promise<void>;
	async prompt(input: string, images?: ImageContent[]): Promise<void>;
	async prompt(input: string | AgentMessage | AgentMessage[], images?: ImageContent[]): Promise<void> {
		if (this.activeRun) {
			throw new Error(
				"Agent is already processing a prompt. Use steer() or followUp() to queue messages, or wait for completion.",
			);
		}
		const messages = this.normalizePromptInput(input, images);
		await this.runPromptMessages(messages);
	}

	/**
	 * 从当前 transcript 继续运行，不新增消息。
	 *
	 * 正常情况下最后一条必须是 user 或 toolResult 消息（assistant 之后没有输入可续）。
	 * 例外是最后一条为 assistant：此时若队列里有排队消息，会把它们当作新一轮的 prompt 执行
	 * （steering 优先于 follow-up），否则抛出 "Cannot continue from message role: assistant"。
	 */
	async continue(): Promise<void> {
		if (this.activeRun) {
			throw new Error("Agent is already processing. Wait for completion before continuing.");
		}

		const lastMessage = this._state.messages[this._state.messages.length - 1];
		if (!lastMessage || this._state.messages.every((message) => message.role === "system")) {
			throw new Error("No messages to continue from");
		}

		if (lastMessage.role === "assistant") {
			// 典型的“在 assistant 上点继续”：没有用户输入可续，但队列里可能有用户排队的消息。
			const queuedSteering = this.steeringQueue.drain();
			if (queuedSteering.length > 0) {
				// 消息已被手动取出，跳过循环入口的首次 steering 轮询；否则 one-at-a-time 模式下
				// 剩余消息会被立刻塞进第一次请求，破坏逐条注入的语义。
				await this.runPromptMessages(queuedSteering, { skipInitialSteeringPoll: true });
				return;
			}

			const queuedFollowUps = this.followUpQueue.drain();
			if (queuedFollowUps.length > 0) {
				await this.runPromptMessages(queuedFollowUps);
				return;
			}

			throw new Error("Cannot continue from message role: assistant");
		}

		await this.runContinuation();
	}

	/** 把重载的输入统一成 `AgentMessage[]`：字符串补成一条带图片的 user 消息，单条消息包成数组。 */
	private normalizePromptInput(
		input: string | AgentMessage | AgentMessage[],
		images?: ImageContent[],
	): AgentMessage[] {
		if (Array.isArray(input)) {
			return input;
		}

		if (typeof input !== "string") {
			return [input];
		}

		const content: Array<TextContent | ImageContent> = [{ type: "text", text: input }];
		if (images && images.length > 0) {
			content.push(...images);
		}
		return [{ role: "user", content, timestamp: Date.now() }];
	}

	/**
	 * 以“新 prompt”方式运行：消息先进入上下文并补发 `message_start`/`message_end` 事件，
	 * 然后循环开始请求模型。
	 */
	private async runPromptMessages(
		messages: AgentMessage[],
		options: { skipInitialSteeringPoll?: boolean } = {},
	): Promise<void> {
		await this.runWithLifecycle(async (signal) => {
			await runAgentLoop(
				messages,
				this.createContextSnapshot(),
				this.createLoopConfig(options),
				(event) => this.processEvents(event),
				signal,
				this.streamFunction,
			);
		});
	}

	/** 不新增消息，直接在现有 transcript 上再请求一次；用于最后一条是 user/toolResult 的场景。 */
	private async runContinuation(): Promise<void> {
		await this.runWithLifecycle(async (signal) => {
			await runAgentLoopContinue(
				this.createContextSnapshot(),
				this.createLoopConfig(),
				(event) => this.processEvents(event),
				signal,
				this.streamFunction,
			);
		});
	}

	/**
	 * 复制一份循环可自由修改的上下文快照（顶层数组浅拷贝）。
	 *
	 * 约定：Agent 的 `state.messages` 只通过事件（`processEvents`）更新；循环拿到快照后
	 * 往里 append 消息不会绕过事件流程，订阅者始终能看到完整、有序的 transcript。
	 */
	private createContextSnapshot(): AgentContext {
		return {
			messages: this._state.messages.slice(),
			tools: this._state.tools.slice(),
		};
	}

	/**
	 * 把 Agent 的公开字段和队列适配成循环需要的配置。
	 *
	 * 每次运行开始时调用一次：运行中途替换的钩子/模型/工具策略不会自动生效，需要等下一次运行。
	 */
	private createLoopConfig(options: { skipInitialSteeringPoll?: boolean } = {}): AgentLoopConfig {
		let skipInitialSteeringPoll = options.skipInitialSteeringPoll === true;
		return {
			model: this._state.model,
			// "off" 在循环配置里用 undefined 表示；保持该约定，避免把字符串 "off" 当成思考级别传给 provider。
			reasoning: this._state.thinkingLevel === "off" ? undefined : this._state.thinkingLevel,
			sessionId: this.sessionId,
			onPayload: this.onPayload,
			onResponse: this.onResponse,
			onProviderStreamEvent: this.onProviderStreamEvent,
			transport: this.transport,
			thinkingBudgets: this.thinkingBudgets,
			maxRetryDelayMs: this.maxRetryDelayMs,
			identity: this.identity,
			toolExecution: this.toolExecution,
			beforeToolCall: this.beforeToolCall,
			afterToolCall: this.afterToolCall,
			finishTurn: this.finishTurn,
			prepareRequest: this.prepareRequest,
			// 优先使用带上下文的钩子；旧钩子只接收 signal，保留兼容。两者都未设置时传 undefined，
			// 循环就不会调用（见 agent-loop.ts 的轮次准备逻辑）。
			prepareNextTurn:
				this.prepareNextTurnWithContext || this.prepareNextTurn
					? async (context) => {
							if (this.prepareNextTurnWithContext) {
								return await this.prepareNextTurnWithContext(context, this.signal);
							}
							return await this.prepareNextTurn?.(this.signal);
						}
					: undefined,
			convertToLlm: this.convertToLlm,
			transformContext: this.transformContext,
			getApiKey: this.getApiKey,
			// 循环在每轮结束后轮询 steering：取出的消息作为新一轮的 user 消息注入。
			getSteeringMessages: async () => {
				if (skipInitialSteeringPoll) {
					skipInitialSteeringPoll = false;
					return [];
				}
				return this.steeringQueue.drain();
			},
			// 只在循环本来要结束时轮询 follow-up：它决定是否再跑一轮。
			getFollowUpMessages: async () => this.followUpQueue.drain(),
		};
	}

	/**
	 * 把一次运行统一包进生命周期：互斥检查、`activeRun`、运行期字段、失败兜底与收尾。
	 *
	 * 任何 `executor` 抛出的错误都会转成失败消息事件（见 `handleRunFailure`），不会向调用方 reject；
	 * 成功或失败最终都会进入 `finishRun()`。
	 */
	private async runWithLifecycle(executor: (signal: AbortSignal) => Promise<void>): Promise<void> {
		// 防御性检查：prompt()/continue()/reset() 已先检查过；这里再兜底，避免内部路径绕过。
		if (this.activeRun) {
			throw new Error("Agent is already processing.");
		}

		const abortController = new AbortController();
		// 手动创建 deferred：finishRun() 需要 resolve 一个在运行开始前就已返回给 waitForIdle 的 promise。
		let resolvePromise = () => {};
		const promise = new Promise<void>((resolve) => {
			resolvePromise = resolve;
		});
		this.activeRun = { promise, resolve: resolvePromise, abortController };

		// 运行期字段只在这里和 processEvents 里改：进入运行时先清掉上一次残留的流式消息与错误。
		this._state.isStreaming = true;
		this._state.streamingMessage = undefined;
		this._state.errorMessage = undefined;

		try {
			await executor(abortController.signal);
		} catch (error) {
			// 失败也要走完整事件生命周期：合成错误 assistant 消息后依次发 message_start/message_end/
			// turn_end/agent_end，订阅者无需区分正常路径与失败路径。
			await this.handleRunFailure(error, abortController.signal.aborted);
		} finally {
			// 无论成功失败都收尾：清运行期字段、唤醒 waitForIdle、释放 activeRun。
			this.finishRun();
		}
	}

	/**
	 * 把一次运行失败合成为一条 assistant 错误消息，并按正常轮次的顺序发送事件。
	 *
	 * 空文本是刻意的：失败信息放在 `errorMessage`，UI 通过 `stopReason` 区分错误与中断；
	 * 零用量保证消息形状完整，下游可安全读取 usage/cost。
	 */
	private async handleRunFailure(error: unknown, aborted: boolean): Promise<void> {
		const failureMessage = {
			role: "assistant",
			content: [{ type: "text", text: "" }],
			api: this._state.model.api,
			provider: this._state.model.provider,
			model: this._state.model.id,
			usage: EMPTY_USAGE,
			stopReason: aborted ? "aborted" : "error",
			errorMessage: error instanceof Error ? error.message : String(error),
			timestamp: Date.now(),
		} satisfies AgentMessage;
		await this.processEvents({ type: "message_start", message: failureMessage });
		await this.processEvents({ type: "message_end", message: failureMessage });
		await this.processEvents({ type: "turn_end", message: failureMessage, toolResults: [] });
		await this.processEvents({ type: "agent_end", messages: [failureMessage] });
	}

	/**
	 * 运行收尾：清掉运行期字段并唤醒 `waitForIdle`。
	 *
	 * 必须在 `agent_end` 的监听器都结束之后调用，否则调用方会在监听器尚未完成时就看到空闲。
	 */
	private finishRun(): void {
		this._state.isStreaming = false;
		this._state.streamingMessage = undefined;
		this._state.pendingToolCalls = new Set<string>();
		this.activeRun?.resolve();
		this.activeRun = undefined;
	}

	/**
	 * 先把循环事件归并进内部状态，再按订阅顺序 await 每个监听器。
	 *
	 * `agent_end` 只表示不会再有循环事件：要等该事件的所有监听器结束、`finishRun()` 清掉运行期
	 * 状态之后，这次运行才真正算 idle。
	 */
	private async processEvents(event: AgentEvent): Promise<void> {
		switch (event.type) {
			case "message_start":
				// 流式消息先挂在 state 上，UI 可据此渲染“正在生成”占位。
				this._state.streamingMessage = event.message;
				break;

			case "message_update":
				// 增量只更新流式消息；它尚未进入 transcript。
				this._state.streamingMessage = event.message;
				break;

			case "message_end":
				// 流结束后才落进 transcript；此时它才成为会话历史的一部分。
				this._state.streamingMessage = undefined;
				this._state.messages.push(event.message);
				break;

			case "tool_execution_start": {
				// 用新 Set 替换而不是原地增删：订阅者拿到的 pendingToolCalls 是不可变快照，便于 diff。
				const pendingToolCalls = new Set(this._state.pendingToolCalls);
				pendingToolCalls.add(event.toolCallId);
				this._state.pendingToolCalls = pendingToolCalls;
				break;
			}

			case "tool_execution_end": {
				// 同样替换整个 Set，保证快照语义一致。
				const pendingToolCalls = new Set(this._state.pendingToolCalls);
				pendingToolCalls.delete(event.toolCallId);
				this._state.pendingToolCalls = pendingToolCalls;
				break;
			}

			case "turn_end":
				// 本轮失败（error/aborted）时把错误暴露到 state.errorMessage；运行失败路径也经过这里。
				if (event.message.role === "assistant" && event.message.errorMessage) {
					this._state.errorMessage = event.message.errorMessage;
				}
				break;

			case "agent_end":
				// 清掉可能残留的流式消息（例如用户中断导致流没有正常收尾）。
				this._state.streamingMessage = undefined;
				break;
		}

		// 事件只能来自一次运行；没有 activeRun 说明调用方绕过了生命周期，直接报错而不是静默处理。
		const signal = this.activeRun?.abortController.signal;
		if (!signal) {
			throw new Error("Agent listener invoked outside active run");
		}
		// 串行 await：保证前一个订阅者处理完再投递下一个，UI 不会看到乱序事件。
		for (const listener of this.listeners) {
			await listener(event, signal);
		}
	}
}
