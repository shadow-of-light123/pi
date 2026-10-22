/**
 * agent-core 的主循环：模型请求 -> 工具执行 -> 结果回填，直到模型不再调用工具。
 *
 * 为什么内部只处理 `AgentMessage`：
 * - `AgentMessage` 是应用可扩展的消息联合类型，允许出现 UI 通知、自定义消息等 provider
 *   不认识的角色。循环内部统一持有这种类型，只有真正调用模型前才通过 `convertToLlm`
 *   转成 `Message[]`（见 `streamAssistantResponse`），这样过滤/转换只发生在一个边界上。
 *
 * 两条对外路径：
 * - `agentLoop` / `agentLoopContinue` 返回 `EventStream<AgentEvent, AgentMessage[]>`，
 *   调用方可以边消费事件边渲染，循环不需要知道界面是否存在。
 * - `runAgentLoop` / `runAgentLoopContinue` 直接 await 完成并返回新消息，供 Agent 类把
 *   同一次运行纳入自己的生命周期与错误处理。
 *
 * 一轮（turn）的定义：一次 assistant 响应 + 它请求的全部工具调用和工具结果。
 */

import {
	type AssistantMessage,
	type ClientIdentity,
	// 带缓冲的异步事件流：消费慢时事件排队，`agent_end` 被 isComplete 用来判定收尾。
	EventStream,
	// 从 transcript 的 system 消息回放“当前已声明哪些工具”。
	getCurrentTools,
	// 对比两份工具声明，得到 added/removed 增量。
	getToolStateChanges,
	// 请求身份按字段合并，后面的来源覆盖前面的。
	mergeClientIdentity,
	// 把带 systemPrompt/tools 的 Context 折叠成以 system 消息开头的 TranscriptContext。
	normalizeContext,
	PI_ATTRIBUTION,
	type SystemMessage,
	type ToolResultMessage,
	type ToolStateChanges,
	// 去掉执行/渲染专用字段，只保留模型可见的 name/description/parameters。
	toToolDeclaration,
	// TypeBox 校验并做类型转换（例如字符串转数字）；失败时抛错。
	validateToolArguments,
} from "@earendil-works/pi-ai";
// 宿主通过 setDefaultStreamFn 注入默认模型运行时，agent-core 因此不必依赖 provider 目录。
import { getDefaultStreamFn } from "./stream-fn.ts";
import type {
	AgentContext,
	AgentEvent,
	AgentLoopConfig,
	AgentMessage,
	AgentTool,
	AgentToolCall,
	AgentToolCallOutcome,
	AgentToolResult,
	PrepareNextTurnContext,
	StreamFn,
} from "./types.ts";

/**
 * 事件出口。返回值可以是 Promise：上层（如 Agent）用它把事件串行投递到 UI/状态层，
 * `await emit` 保证上一个事件处理完才继续，避免并发更新状态。
 */
export type AgentEventSink = (event: AgentEvent) => Promise<void> | void;

/**
 * 默认请求身份：在 pi-ai 的默认值之上把归因信息标记为 pi。
 * `AgentLoopConfig.identity` 会按字段覆盖它（见 `streamAssistantResponse`），
 * 因此宿主只需设置自己关心的字段，其余保持默认。
 */
export const DEFAULT_AGENT_IDENTITY: ClientIdentity = { attribution: PI_ATTRIBUTION };

/**
 * 用一条新 prompt 启动循环。
 *
 * prompt 会先进入上下文并补发 `message_start`/`message_end` 事件，这样订阅者看到的
 * transcript 与循环内部一致。返回的 stream 立即可用：事件由 `runAgentLoop` 异步推送，
 * 消费方不必等循环结束才开始迭代。
 *
 * `stream.end(messages)` 是第二重收尾：`agent_end` 本身已让 `EventStream` 完成并解析出
 * 最终结果，这里再显式 end 一次，确保正常返回时等待中的消费者一定被唤醒。
 */
export function agentLoop(
	prompts: AgentMessage[],
	context: AgentContext,
	config: AgentLoopConfig,
	signal: AbortSignal | undefined,
	streamFn: StreamFn,
): EventStream<AgentEvent, AgentMessage[]> {
	const stream = createAgentStream();

	// 用 void 丢弃 Promise：调用方只消费事件流，不等待循环完成；
	// 模型失败按 StreamFn 契约体现在错误/中断事件里，而不是靠 reject 传播。
	void runAgentLoop(
		prompts,
		context,
		config,
		async (event) => {
			stream.push(event);
		},
		signal,
		streamFn,
	).then((messages) => {
		stream.end(messages);
	});

	return stream;
}

/**
 * 不添加新消息，直接从当前上下文继续。
 * 用于重试，或上一次运行因错误/中断结束后再发起一次请求。
 *
 * 前置条件：最后一条消息经 `convertToLlm` 后必须是 `user` 或 `toolResult`。
 * 这里只能检查 `role`，因为 `convertToLlm` 每轮只在 `streamAssistantResponse` 里调用一次，
 * 转换结果此刻还不存在；若最终仍是 assistant，provider 会拒绝请求，因此提前给出明确错误。
 */
export function agentLoopContinue(
	context: AgentContext,
	config: AgentLoopConfig,
	signal: AbortSignal | undefined,
	streamFn: StreamFn,
): EventStream<AgentEvent, AgentMessage[]> {
	if (context.messages.length === 0) {
		throw new Error("Cannot continue: no messages in context");
	}

	if (context.messages[context.messages.length - 1].role === "assistant") {
		throw new Error("Cannot continue from message role: assistant");
	}

	const stream = createAgentStream();

	void runAgentLoopContinue(
		context,
		config,
		async (event) => {
			stream.push(event);
		},
		signal,
		streamFn,
	).then((messages) => {
		stream.end(messages);
	});

	return stream;
}

/**
 * `agentLoop` 的实际实现。与 stream 版本的区别：这里直接 await 整个运行并返回新消息，
 * 不经过 EventStream，方便上层把运行纳入自己的生命周期/错误处理。
 *
 * `newMessages` 是本次调用新产生的消息（prompt 运行包含初始 prompt），
 * 不含调用前就存在的历史；调用方据此追加持久化状态。
 */
export async function runAgentLoop(
	prompts: AgentMessage[],
	context: AgentContext,
	config: AgentLoopConfig,
	emit: AgentEventSink,
	signal: AbortSignal | undefined,
	streamFn: StreamFn,
): Promise<AgentMessage[]> {
	// 先补一条 system 消息，把“可执行工具”与 transcript 中已声明工具之间的差异告诉模型。
	// 初始 prompt 里可能本来就带 system 消息（例如压缩摘要），同样走增量声明逻辑。
	const initialMessages = declareToolChanges(context, prompts);
	// initialMessages 同时进入返回值（newMessages）和本次运行的上下文。
	const newMessages: AgentMessage[] = [...initialMessages];
	const currentContext: AgentContext = {
		...context,
		// 拷贝顶层数组：循环会往 currentContext.messages 里追加，不能污染调用方传入的快照。
		messages: [...context.messages, ...initialMessages],
	};

	// 生命周期顺序固定：agent_start -> turn_start -> 初始消息事件 -> 循环内各轮事件。
	await emit({ type: "agent_start" });
	await emit({ type: "turn_start" });
	for (const message of initialMessages) {
		await emit({ type: "message_start", message });
		await emit({ type: "message_end", message });
	}

	// streamFn 允许调用方省略，此时用宿主安装的默认模型运行时；
	// 两者都没有时 getDefaultStreamFn 会抛出明确错误。
	await runLoop(currentContext, newMessages, config, signal, emit, streamFn ?? getDefaultStreamFn());
	return newMessages;
}

/**
 * `agentLoopContinue` 的实际实现。与 prompt 运行不同，这里不补发初始消息事件——
 * 历史消息在之前的运行里已经发过；本次只发出 agent_start/turn_start。
 *
 * 入口校验在这里重复一次：直接调用（不经过 stream 包装）的调用方同样需要尽早失败。
 */
export async function runAgentLoopContinue(
	context: AgentContext,
	config: AgentLoopConfig,
	emit: AgentEventSink,
	signal: AbortSignal | undefined,
	streamFn: StreamFn,
): Promise<AgentMessage[]> {
	if (context.messages.length === 0) {
		throw new Error("Cannot continue: no messages in context");
	}

	if (context.messages[context.messages.length - 1].role === "assistant") {
		throw new Error("Cannot continue from message role: assistant");
	}

	// 续跑不产生“历史外”的新消息，newMessages 只收集本次运行新增的部分。
	const newMessages: AgentMessage[] = [];
	// 浅拷贝上下文：messages 数组会被 runLoop 原地追加。
	const currentContext: AgentContext = { ...context };

	await emit({ type: "agent_start" });
	await emit({ type: "turn_start" });

	await runLoop(currentContext, newMessages, config, signal, emit, streamFn ?? getDefaultStreamFn());
	return newMessages;
}

/**
 * 构造对外事件流。
 * - `isComplete`：看到 `agent_end` 就认为流结束，并解析最终结果，消费方无需等待 `end()`；
 * - `extractResult`：最终结果取 `agent_end.messages`，与 `runAgentLoop` 的返回值一致。
 */
function createAgentStream(): EventStream<AgentEvent, AgentMessage[]> {
	return new EventStream<AgentEvent, AgentMessage[]>(
		(event: AgentEvent) => event.type === "agent_end",
		(event: AgentEvent) => (event.type === "agent_end" ? event.messages : []),
	);
}

/**
 * `agentLoop` 与 `agentLoopContinue` 共用的主循环。
 *
 * 之所以是两层循环：“继续”有两种来源，优先级不同。
 *
 * ```
 * while (true) {                                              // 外层：本来要停下时的兜底
 *   while (hasMoreToolCalls || pendingMessages.length > 0) {  // 内层：一轮对话
 *     准备上下文 -> 请求模型 -> 执行工具 -> 询问 finishTurn
 *   }
 *   follow-up 消息？ -> 变成 pending，继续外层
 *   finishTurn 要求 continue？ -> 不新增消息，再请求一次
 *   否则退出
 * }
 * ```
 *
 * 内层每轮固定顺序：
 * 1. `prepareNextTurn`（首轮之后）替换上下文/模型/思考级别，并补发 `turn_start`；
 * 2. 追加 prepared 与 steering 消息（同样经过工具声明增量处理）；
 * 3. `prepareRequest` 在发请求前做最后一次上下文替换；
 * 4. `streamAssistantResponse` 流式产出 assistant 消息；
 * 5. 执行工具调用并把结果写回上下文；
 * 6. `finishTurn` 决定结束还是再请求一次，随后轮询 steering 消息。
 *
 * 状态变量：
 * - `lastCompletedTurn`：最近一轮的完整快照；为 undefined 说明是首轮（`turn_start` 已由入口发过）。
 * - `hasMoreToolCalls`：上一批工具没有要求提前终止，因此还要继续对话。
 * - `pendingMessages`：待注入的 steering/follow-up 消息，注入时才会产生消息事件。
 * - `explicitContinuation`：`finishTurn` 明确要求再请求一次，但没有工具/队列消息满足它。
 */
async function runLoop(
	initialContext: AgentContext,
	newMessages: AgentMessage[],
	initialConfig: AgentLoopConfig,
	signal: AbortSignal | undefined,
	emit: AgentEventSink,
	streamFunction: StreamFn,
): Promise<void> {
	let currentContext = initialContext;
	// config 是 let：prepareNextTurn/prepareRequest 可以替换模型和思考级别，
	// 替换后的值对后续所有请求生效。
	let config = initialConfig;
	let lastCompletedTurn: PrepareNextTurnContext | undefined;
	let explicitContinuation = false;
	// 入口先看一下 steering 队列：用户可能在 await 期间已经输入了新消息。
	// 这些消息不会立刻注入，而是在内层第一轮和 prepared 消息一起追加。
	let pendingMessages: AgentMessage[] = (await config.getSteeringMessages?.()) || [];

	// 外层：模型本来要停下时，仍可能因 follow-up 消息或 finishTurn 的决定继续。
	while (true) {
		// 置 true 保证内层至少发起一轮请求（即使没有任何排队消息）。
		let hasMoreToolCalls = true;

		// 内层：只要还有工具结果要回填，或者有排队消息要注入，就继续请求模型。
		while (hasMoreToolCalls || pendingMessages.length > 0) {
			let preparedMessages: AgentMessage[] = [];
			// 首轮不调用：入口已经发过 turn_start，也还没有“已完成的上一轮”可供准备。
			if (lastCompletedTurn) {
				// 宿主可以在这里压缩上下文、切换模型/思考级别，或追加 system 消息。
				const nextTurnSnapshot = await config.prepareNextTurn?.(lastCompletedTurn);
				if (nextTurnSnapshot) {
					currentContext = nextTurnSnapshot.context ?? currentContext;
					preparedMessages = nextTurnSnapshot.messages ?? [];
					config = {
						...config,
						model: nextTurnSnapshot.model ?? config.model,
						// "off" 在 AgentLoopConfig 里用 undefined 表示；这里保持该约定，
						// 避免下游把一个字符串 "off" 当成有效的思考级别传给 provider。
						reasoning:
							nextTurnSnapshot.thinkingLevel === undefined
								? config.reasoning
								: nextTurnSnapshot.thinkingLevel === "off"
									? undefined
									: nextTurnSnapshot.thinkingLevel,
					};
				}
				// 准备可能耗时很久（例如压缩），期间用户可能又输入了 steering；
				// 只在之前没取到消息时才补取：one-at-a-time 模式下提前取会让两条消息挤进同一轮。
				if (pendingMessages.length === 0) {
					pendingMessages = (await config.getSteeringMessages?.()) || [];
				}
				await emit({ type: "turn_start" });
			}

			// prepared（压缩摘要等）在前、steering 在后，一起追加。declareToolChanges 负责
			// 在需要时插入 system 消息，声明工具集合的变化。
			for (const message of declareToolChanges(currentContext, [...preparedMessages, ...pendingMessages])) {
				await emit({ type: "message_start", message });
				await emit({ type: "message_end", message });
				currentContext.messages.push(message);
				newMessages.push(message);
			}
			pendingMessages = [];

			// 每次请求前都会调用（包括首轮），此时待注入消息已经追加并发出事件。
			// 返回值只影响本次及之后的请求，不会触发队列轮询。
			const requestUpdate = await config.prepareRequest?.(
				{
					context: currentContext,
					model: config.model,
					thinkingLevel: config.reasoning ?? "off",
				},
				signal,
			);
			if (requestUpdate) {
				currentContext = requestUpdate.context ?? currentContext;
				config = {
					...config,
					model: requestUpdate.model ?? config.model,
					// 与 prepareNextTurn 相同的 "off" -> undefined 约定。
					reasoning:
						requestUpdate.thinkingLevel === undefined
							? config.reasoning
							: requestUpdate.thinkingLevel === "off"
								? undefined
								: requestUpdate.thinkingLevel,
				};
			}

			// 模型响应：流式事件已经由 streamAssistantResponse 发出，这里只把最终消息记进 newMessages。
			const message = await streamAssistantResponse(currentContext, config, signal, emit, streamFunction);
			newMessages.push(message);

			// 错误/中断是硬退出：不再尝试工具或队列，但仍通知 finishTurn/turn_end/agent_end，
			// 让宿主记录 usage、错误状态并做清理。
			if (message.stopReason === "error" || message.stopReason === "aborted") {
				lastCompletedTurn = {
					message,
					toolResults: [],
					context: currentContext,
					newMessages,
				};
				await config.finishTurn?.(lastCompletedTurn, signal);
				await emit({ type: "turn_end", message, toolResults: [] });
				await emit({ type: "agent_end", messages: newMessages });
				return;
			}

			// 只有 toolCall 内容块需要执行；文本/思考块由 UI 从消息事件里消费。
			const toolCalls = message.content.filter((c) => c.type === "toolCall");

			const toolResults: ToolResultMessage[] = [];
			// 默认“没有工具调用”；只有执行完一批且没有被要求提前终止时才继续。
			hasMoreToolCalls = false;
			if (toolCalls.length > 0) {
				// stopReason === "length" 表示输出被 token 上限截断：流式 tool call 的参数是
				// 尽力补全的 JSON，可能能解析却缺字段，执行它们会产生副作用或错误结果，
				// 因此整批标记为失败，让模型重新发起。
				const executedToolBatch =
					message.stopReason === "length"
						? await failToolCallsFromTruncatedMessage(toolCalls, emit)
						: await executeToolCalls(currentContext, message, config, signal, emit);
				toolResults.push(...executedToolBatch.messages);
				// 提前终止只有整批工具结果都设置 terminate 才生效（见 shouldTerminateToolBatch）。
				hasMoreToolCalls = !executedToolBatch.terminate;

				// 结果按 assistant 源顺序写回 transcript；并行执行时完成顺序可能不同，
				// 但 toolResult 消息必须与前面的 toolCall 顺序严格对应。
				for (const result of toolResults) {
					currentContext.messages.push(result);
					newMessages.push(result);
				}
			}

			// 记录已完成的一轮，供 prepareNextTurn/finishTurn 读取完整快照。
			lastCompletedTurn = {
				message,
				toolResults,
				context: currentContext,
				newMessages,
			};
			// finishTurn 在 turn_end 之前调用，可结束本次运行，也可强制再请求一次。
			const decision = await config.finishTurn?.(lastCompletedTurn, signal);
			await emit({ type: "turn_end", message, toolResults });

			if (decision?.action === "end") {
				await emit({ type: "agent_end", messages: newMessages });
				return;
			}

			// “continue” 只在没有自然理由继续时才需要特殊处理：
			// 有工具调用或 steering 消息时，内层本来就会再跑一轮。
			explicitContinuation = decision?.action === "continue";
			pendingMessages = (await config.getSteeringMessages?.()) || [];
			if (hasMoreToolCalls || pendingMessages.length > 0) {
				explicitContinuation = false;
			}
		}

		// 内层结束说明模型没有工具调用、也没有 steering 消息，本可以停下。
		// follow-up 是“等 agent 干完活再处理”的消息，优先级高于 finishTurn 的 continue。
		const followUpMessages = (await config.getFollowUpMessages?.()) || [];
		if (followUpMessages.length > 0) {
			// 作为 pending 交给内层消费；外层下一轮会重新进入内层。
			explicitContinuation = false;
			pendingMessages = followUpMessages;
			continue;
		}

		// finishTurn 要求继续，但没有消息可注入：用一个“仅上下文”的轮次再请求一次，
		// 让模型有机会在现有 transcript 上自行继续，而不新增用户消息。
		if (explicitContinuation) {
			explicitContinuation = false;
			continue;
		}

		// 没有任何继续的理由，退出外层循环。
		break;
	}

	// 正常退出（非上面提前 return 的路径）统一发 agent_end，携带本次运行的全部新消息。
	await emit({ type: "agent_end", messages: newMessages });
}

/**
 * 向模型声明工具集合的变化。
 *
 * 关键区分：`context.tools` 是运行时“能执行”的工具；模型“被允许调用”的工具由 transcript
 * 里的 system 消息声明。两者不一致时，必须在请求前把差异写成 `toolsAdded` / `toolsRemoved`，
 * 否则模型会按旧声明调用已经不存在的工具。
 *
 * 两种处理方式：
 * - 待追加消息里已有 system 消息（常见于压缩摘要）：把它的工具字段当作“意图”，
 *   用“已提交 transcript 与当前可执行集合的差异”替换它，保证回放结果恰好等于 `context.tools`。
 * - 没有这样的 system 消息：插入一条新的，位置在第一条非 system 待追加消息之前，
 *   让工具声明先于用户消息被模型看到。
 *
 * 差异基于“已提交 transcript + 待追加消息（其中 system 消息的工具字段已清空）”计算，
 * 避免把尚未生效的声明算进基线。
 */
function declareToolChanges(context: AgentContext, pendingMessages: AgentMessage[]): AgentMessage[] {
	// 从后往前找：只让最后一条 system 消息承载本次工具增量；
	// 更早的 system 消息保持调用方给定的内容，不在这里改写。
	let systemIndex = -1;
	for (let i = pendingMessages.length - 1; i >= 0; i--) {
		if (pendingMessages[i].role === "system") {
			systemIndex = i;
			break;
		}
	}
	const pending = pendingMessages[systemIndex] as SystemMessage | undefined;
	// baseline：待处理的 system 消息清空工具字段后的消息列表，代表“提交前”的工具状态。
	const baseline = pending
		? pendingMessages.map((message, index) =>
				index === systemIndex ? withToolChanges(pending, NO_CHANGES) : message,
			)
		: pendingMessages;
	// 同名但定义变化的工具会被算作“一删一增”，因此 unchanged 只看两个列表是否为空。
	const changes = getToolStateChanges(
		getCurrentTools([...context.messages, ...baseline]),
		(context.tools ?? []).map(toToolDeclaration),
	);
	const unchanged = changes.toolsAdded.length === 0 && changes.toolsRemoved.length === 0;

	if (pending) {
		// 调用方给的 system 消息本来就没有工具变化，且实际也没有变化：原样返回，保留对象身份。
		if (unchanged && !pending.toolsAdded?.length && !pending.toolsRemoved?.length) return pendingMessages;
		return baseline.map((message, index) => (index === systemIndex ? withToolChanges(pending, changes) : message));
	}
	if (unchanged) return pendingMessages;
	// 新建一条空内容的 system 消息，只承载工具增量；content 为空不会影响提示词文本。
	const update = withToolChanges({ role: "system", content: "", timestamp: Date.now() }, changes);
	// 插到第一条非 system 消息之前；全是 system 消息时放到末尾。
	const insertIndex = pendingMessages.findIndex((message) => message.role !== "system");
	const index = insertIndex === -1 ? pendingMessages.length : insertIndex;
	return [...pendingMessages.slice(0, index), update, ...pendingMessages.slice(index)];
}

/** 复用同一个空增量对象，避免每次比较都分配。 */
const NO_CHANGES: ToolStateChanges = { toolsAdded: [], toolsRemoved: [] };

/**
 * 复制一条 system 消息，并把工具字段替换为 `changes`。
 *
 * 空列表不写回字段：transcript 里的“无变化”应表现为字段缺失而不是空数组，
 * 这样回放、序列化和对象比较都更稳定。
 */
function withToolChanges(message: SystemMessage, { toolsAdded, toolsRemoved }: ToolStateChanges): SystemMessage {
	// 解构丢弃旧工具字段，避免 `...rest` 把它们又带回来。
	const { toolsAdded: _added, toolsRemoved: _removed, ...rest } = message;
	return {
		...rest,
		...(toolsAdded.length > 0 ? { toolsAdded } : {}),
		...(toolsRemoved.length > 0 ? { toolsRemoved } : {}),
	};
}

/**
 * 从 LLM 流式产出一条 assistant 消息，并把它写入 `context.messages`。
 *
 * 这里是 `AgentMessage` 与 provider `Message` 的唯一边界：
 * 1. `transformContext`：应用层整理上下文（裁剪旧消息、注入外部内容），仍处理 AgentMessage；
 * 2. `convertToLlm`：过滤/转换 provider 不认识的消息（UI 通知等）；
 * 3. `normalizeContext`：类型上补成 `TranscriptContext`。system prompt 与工具声明已经
 *    存在于 transcript 的 system 消息里，因此这里不再单独传 systemPrompt/tools。
 *
 * 流式状态机的两个约束：
 * - `start` 到达时把 partial 消息放进上下文，后续增量原地更新同一个下标，
 *   这样 UI 从 `message_update` 拿到的始终是“最新整体”，不需要自己拼接增量。
 * - `done`/`error` 到达时用最终消息替换 partial 并发出 `message_end`；若流函数没有发
 *   `start` 就直接给出最终结果，则补发一对 `message_start`/`message_end`，保证事件成对。
 */
async function streamAssistantResponse(
	context: AgentContext,
	config: AgentLoopConfig,
	signal: AbortSignal | undefined,
	emit: AgentEventSink,
	streamFunction: StreamFn,
): Promise<AssistantMessage> {
	// 未配置时直接复用原数组，避免大 transcript 的无谓拷贝。
	let messages = context.messages;
	if (config.transformContext) {
		messages = await config.transformContext(messages, signal);
	}

	// 契约要求 convertToLlm 不抛错；这里不做 try/catch，失败会中断低层循环。
	const llmMessages = await config.convertToLlm(messages);

	// 这次调用实际上只做类型收窄：没有任何 systemPrompt/tools 需要折叠。
	const llmContext = normalizeContext({ messages: llmMessages });

	// 每次请求都重新取 key：OAuth token 可能在长时间工具执行期间过期。
	// getApiKey 的返回值优先于静态 apiKey。
	const resolvedApiKey =
		(config.getApiKey ? await config.getApiKey(config.model.provider) : undefined) || config.apiKey;

	// 把 AgentLoopConfig 继承自 SimpleStreamOptions 的字段原样透传（温度、重试、传输层等），
	// 只覆盖由循环自己决定的 apiKey/identity/signal。
	const response = await streamFunction(config.model, llmContext, {
		...config,
		apiKey: resolvedApiKey,
		// 宿主身份按字段覆盖默认的 pi 归因。
		identity: mergeClientIdentity(DEFAULT_AGENT_IDENTITY, config.identity),
		signal,
	});
	// 最终消息上补记“本次请求要求的思考级别”，因为 stream 函数不保证回填它；
	// 会话回放和 UI 都需要知道当时用的是哪一级。
	const result = async () => Object.assign(await response.result(), { thinkingLevel: config.reasoning ?? "off" });

	// partial 是否已经进入上下文：决定最终消息是“替换”还是“追加”。
	let partialMessage: AssistantMessage | null = null;
	let addedPartial = false;

	for await (const event of response) {
		switch (event.type) {
			case "start":
				// partial 先入上下文，后续事件只替换最后一项，保证“正在流式生成的消息”始终在 transcript 里。
				partialMessage = event.partial;
				context.messages.push(partialMessage);
				addedPartial = true;
				// 发给订阅者的是浅拷贝，避免外部持有引用后被后续原地更新“穿越”修改。
				await emit({ type: "message_start", message: { ...partialMessage } });
				break;

			case "text_start":
			case "text_delta":
			case "text_end":
			case "thinking_start":
			case "thinking_delta":
			case "thinking_end":
			case "toolcall_start":
			case "toolcall_delta":
			case "toolcall_end":
				// 所有增量事件只做两件事：把上下文里的 partial 换成最新版，再把事件转给订阅者。
				if (partialMessage) {
					partialMessage = event.partial;
					context.messages[context.messages.length - 1] = partialMessage;
					await emit({
						type: "message_update",
						assistantMessageEvent: event,
						message: { ...partialMessage },
					});
				}
				break;

			case "done":
			case "error": {
				// response.result() 由 done/error 事件解析，且事件流会统一补上 durationMs。
				const finalMessage = await result();
				if (addedPartial) {
					context.messages[context.messages.length - 1] = finalMessage;
				} else {
					context.messages.push(finalMessage);
				}
				if (!addedPartial) {
					// 流函数没发 start：补一条开始事件，保证 start/end 成对。
					await emit({ type: "message_start", message: { ...finalMessage } });
				}
				await emit({ type: "message_end", message: finalMessage });
				return finalMessage;
			}
		}
	}

	// 兜底：流实现可能直接 end(result) 而不发 done/error。
	// 此时 result() 仍能解析出最终消息，按同样的规则写入上下文并发出事件。
	const finalMessage = await result();
	if (addedPartial) {
		context.messages[context.messages.length - 1] = finalMessage;
	} else {
		context.messages.push(finalMessage);
		await emit({ type: "message_start", message: { ...finalMessage } });
	}
	await emit({ type: "message_end", message: finalMessage });
	return finalMessage;
}

/**
 * 把被输出 token 上限截断的 assistant 消息里的工具调用全部判为失败。
 *
 * 原因：流式 tool call 的参数是用“尽力抢救”的 JSON 解析器补全的。截断后的参数可能
 * 能通过 schema 校验，却缺少关键字段（例如命令只写了一半）。执行这种调用比不执行更危险，
 * 因此统一返回错误，让模型重新发起完整调用。
 *
 * 仍然发出完整的 tool_execution_start/end 和消息事件：UI 已经显示过工具卡片，
 * 不补齐生命周期会让界面停在“执行中”。`terminate: false` 表示流程照常继续，由模型重试。
 */
async function failToolCallsFromTruncatedMessage(
	toolCalls: AgentToolCall[],
	emit: AgentEventSink,
): Promise<ExecutedToolCallBatch> {
	const messages: ToolResultMessage[] = [];
	for (const toolCall of toolCalls) {
		// 与正常执行保持相同的事件序列，只是结果直接是错误。
		await emit({
			type: "tool_execution_start",
			toolCallId: toolCall.id,
			toolName: toolCall.name,
			args: toolCall.arguments,
		});
		const finalized: FinalizedToolCallOutcome = {
			toolCall,
			result: createErrorToolResult(
				`Tool call "${toolCall.name}" was not executed: the response hit the output token limit, so its arguments may be truncated. Re-issue the tool call with complete arguments.`,
			),
			isError: true,
		};
		await emitToolExecutionEnd(finalized, emit);
		const toolResultMessage = createToolResultMessage(finalized);
		await emitToolResultMessage(toolResultMessage, emit);
		messages.push(toolResultMessage);
	}
	return { messages, terminate: false };
}

/**
 * 执行一条 assistant 消息里的全部工具调用，并选择串行或并行策略。
 *
 * 除了全局 `config.toolExecution`，单个工具还可以用 `executionMode: "sequential"` 要求独占。
 * 只要批里有一个这样的工具，整批就降级为串行——并行批次无法保证它与其他调用不重叠。
 */
async function executeToolCalls(
	currentContext: AgentContext,
	assistantMessage: AssistantMessage,
	config: AgentLoopConfig,
	signal: AbortSignal | undefined,
	emit: AgentEventSink,
): Promise<ExecutedToolCallBatch> {
	const toolCalls = assistantMessage.content.filter((c) => c.type === "toolCall");
	// 先收集再决定：策略取决于整批工具，而不是逐个判断。
	const hasSequentialToolCall = toolCalls.some(
		(tc) => currentContext.tools?.find((t) => t.name === tc.name)?.executionMode === "sequential",
	);
	if (config.toolExecution === "sequential" || hasSequentialToolCall) {
		return executeToolCallsSequential(currentContext, assistantMessage, toolCalls, config, signal, emit);
	}
	return executeToolCallsParallel(currentContext, assistantMessage, toolCalls, config, signal, emit);
}

/**
 * 一批工具调用的执行结果。
 * `messages` 与 assistant 消息里的 toolCall 一一对应且保持源顺序；
 * `terminate` 来自 `shouldTerminateToolBatch`，只有整批都要求提前终止时才为 true。
 */
type ExecutedToolCallBatch = {
	messages: ToolResultMessage[];
	terminate: boolean;
};

/**
 * 串行执行：每个工具走完“准备 -> 执行 -> 收尾 -> 发出结果”再处理下一个。
 * 顺序完全可预测（适合有副作用的工具），代价是总耗时相加。
 */
async function executeToolCallsSequential(
	currentContext: AgentContext,
	assistantMessage: AssistantMessage,
	toolCalls: AgentToolCall[],
	config: AgentLoopConfig,
	signal: AbortSignal | undefined,
	emit: AgentEventSink,
): Promise<ExecutedToolCallBatch> {
	const finalizedCalls: FinalizedToolCallOutcome[] = [];
	const messages: ToolResultMessage[] = [];

	for (const toolCall of toolCalls) {
		// 执行开始事件在准备阶段之前发出：UI 先看到卡片，即使随后参数校验失败也能正常落成错误结果。
		await emit({
			type: "tool_execution_start",
			toolCallId: toolCall.id,
			toolName: toolCall.name,
			args: toolCall.arguments,
		});

		const preparation = await prepareToolCall(currentContext, assistantMessage, toolCall, config, signal);
		let finalized: FinalizedToolCallOutcome;
		if (preparation.kind === "immediate") {
			// 未知工具、参数非法、被 beforeToolCall 拦截或已取消：无需执行。
			finalized = {
				toolCall,
				result: preparation.result,
				isError: preparation.isError,
			};
		} else {
			const executed = await executePreparedToolCall(preparation, signal, emitToolExecutionUpdate(toolCall, emit));
			finalized = await finalizeExecutedToolCall(
				currentContext,
				assistantMessage,
				preparation,
				executed,
				config,
				signal,
			);
		}

		// 串行模式下每次执行结束立刻发结束事件，工具结果消息也按源顺序立即发出。
		await emitToolExecutionEnd(finalized, emit);
		const toolResultMessage = createToolResultMessage(finalized);
		await emitToolResultMessage(toolResultMessage, emit);
		finalizedCalls.push(finalized);
		messages.push(toolResultMessage);

		// 取消后不再开始后续调用；已经完成的调用仍会留下结果。
		if (signal?.aborted) {
			break;
		}
	}

	return {
		messages,
		terminate: shouldTerminateToolBatch(finalizedCalls),
	};
}

/**
 * 并行执行：先串行“预检”，再并发执行。
 *
 * 两阶段的原因：钩子和校验有顺序语义。`beforeToolCall`（常见用途是权限确认）必须按
 * assistant 给出的顺序逐个询问，不能在并发中乱序；真正执行则可以同时进行。
 *
 * 事件有两套顺序：
 * - `tool_execution_end` 在各自完成时发出（完成顺序），让 UI 尽早把卡片标成结束；
 * - 工具结果消息统一在 `Promise.all` 之后按 assistant 源顺序发出，因为 transcript 中
 *   toolResult 必须严格对应前面 toolCall 的顺序。
 */
async function executeToolCallsParallel(
	currentContext: AgentContext,
	assistantMessage: AssistantMessage,
	toolCalls: AgentToolCall[],
	config: AgentLoopConfig,
	signal: AbortSignal | undefined,
	emit: AgentEventSink,
): Promise<ExecutedToolCallBatch> {
	// 预检得到的结果先暂存；可执行的调用暂存为 thunk，等预检全部完成后再一起启动。
	const finalizedCalls: FinalizedToolCallEntry[] = [];

	for (const toolCall of toolCalls) {
		// 预检阶段串行遍历：开始事件也在这里按源顺序发出。
		await emit({
			type: "tool_execution_start",
			toolCallId: toolCall.id,
			toolName: toolCall.name,
			args: toolCall.arguments,
		});

		const preparation = await prepareToolCall(currentContext, assistantMessage, toolCall, config, signal);
		if (preparation.kind === "immediate") {
			// 预检就能得到结果：直接结束该调用，不进入并发阶段。
			const finalized = {
				toolCall,
				result: preparation.result,
				isError: preparation.isError,
			} satisfies FinalizedToolCallOutcome;
			await emitToolExecutionEnd(finalized, emit);
			finalizedCalls.push(finalized);
			if (signal?.aborted) {
				break;
			}
			continue;
		}

		// 包成 thunk 而不是立即执行：避免“边预检边执行”导致 beforeToolCall 与工具执行交错。
		finalizedCalls.push(async () => {
			// 启动前再检查一次取消：预检其他调用期间可能已被中断。
			if (signal?.aborted) {
				const finalized = {
					toolCall,
					result: createErrorToolResult("Operation aborted"),
					isError: true,
				} satisfies FinalizedToolCallOutcome;
				await emitToolExecutionEnd(finalized, emit);
				return finalized;
			}
			const executed = await executePreparedToolCall(preparation, signal, emitToolExecutionUpdate(toolCall, emit));
			const finalized = await finalizeExecutedToolCall(
				currentContext,
				assistantMessage,
				preparation,
				executed,
				config,
				signal,
			);
			await emitToolExecutionEnd(finalized, emit);
			return finalized;
		});
		// 预检期间被取消：不再登记剩余调用。
		if (signal?.aborted) {
			break;
		}
	}

	// Promise.all 保持数组顺序返回结果，因此这里拿到的是 assistant 源顺序，与完成顺序无关。
	const orderedFinalizedCalls = await Promise.all(
		finalizedCalls.map((entry) => (typeof entry === "function" ? entry() : Promise.resolve(entry))),
	);
	// 结果消息在全部执行完成后按源顺序发出；各自的 end 事件已在上面发过。
	const messages: ToolResultMessage[] = [];
	for (const finalized of orderedFinalizedCalls) {
		const toolResultMessage = createToolResultMessage(finalized);
		await emitToolResultMessage(toolResultMessage, emit);
		messages.push(toolResultMessage);
	}

	return {
		messages,
		terminate: shouldTerminateToolBatch(orderedFinalizedCalls),
	};
}

/** 通过预检、可以实际执行的调用；`args` 已通过 schema 校验。 */
type PreparedToolCall = {
	kind: "prepared";
	toolCall: AgentToolCall;
	tool: AgentTool<any>;
	args: unknown;
};

/** 无需执行就能得到结果的调用（未知工具、校验失败、被拦截、已取消）。 */
type ImmediateToolCallOutcome = {
	kind: "immediate";
	result: AgentToolResult<any>;
	isError: boolean;
};

/** `execute()` 的直接输出，附带以单调时钟测量的耗时。 */
type ExecutedToolCallOutcome = {
	result: AgentToolResult<any>;
	isError: boolean;
	durationMs: number;
};

/** `afterToolCall` 处理后的最终结果，与对外事件/消息共用同一形状。 */
type FinalizedToolCallOutcome = AgentToolCallOutcome;

/**
 * 只取 {@link AgentLoopConfig} 里的两个工具调用钩子。
 * 这样 prepare/finalize 以及 `runToolCall` 不必依赖完整配置（模型、转换函数等）。
 */
export type ToolCallHooks = Pick<AgentLoopConfig, "beforeToolCall" | "afterToolCall">;

/** 工具流式中间结果的出口；对应 `tool_execution_update` 事件。 */
type ToolUpdateSink = (partialResult: AgentToolResult<any>) => Promise<void> | void;

/** 并行批次里既可能是已完成的结果，也可能是尚未启动的执行 thunk。 */
type FinalizedToolCallEntry = FinalizedToolCallOutcome | (() => Promise<FinalizedToolCallOutcome>);

/**
 * 提前终止条件：批次非空，且每个最终结果都设置了 `terminate: true`。
 * 只要有一个工具没要求终止，就继续下一轮对话。
 */
function shouldTerminateToolBatch(finalizedCalls: FinalizedToolCallOutcome[]): boolean {
	return finalizedCalls.length > 0 && finalizedCalls.every((finalized) => finalized.result.terminate === true);
}

/**
 * 给工具一个在 schema 校验前修正原始参数的机会（兼容层）。
 * 没有钩子，或钩子返回同一个对象引用时直接复用，避免无意义的拷贝。
 */
function prepareToolCallArguments(tool: AgentTool<any>, toolCall: AgentToolCall): AgentToolCall {
	if (!tool.prepareArguments) {
		return toolCall;
	}
	const preparedArguments = tool.prepareArguments(toolCall.arguments);
	if (preparedArguments === toolCall.arguments) {
		return toolCall;
	}
	return {
		...toolCall,
		arguments: preparedArguments as Record<string, any>,
	};
}

/**
 * 工具调用的预检：解析工具、修正参数、schema 校验、调用 `beforeToolCall`。
 *
 * 返回两种结果之一：
 * - `immediate`：无需执行（工具不存在、参数非法、被钩子拦截、已取消），直接形成错误结果；
 * - `prepared`：可以交给 `executePreparedToolCall` 执行。
 *
 * 这里不抛错：校验失败和钩子异常都转成 immediate 错误结果，避免一个坏调用
 * 让整批工具（甚至整个循环）失败。
 */
async function prepareToolCall(
	currentContext: AgentContext,
	assistantMessage: AssistantMessage,
	toolCall: AgentToolCall,
	config: ToolCallHooks,
	signal: AbortSignal | undefined,
	tools: readonly AgentTool<any>[] = currentContext.tools ?? [],
): Promise<PreparedToolCall | ImmediateToolCallOutcome> {
	const tool = tools.find((t) => t.name === toolCall.name);
	if (!tool) {
		// 模型可能调用了已移除或拼错的工具名；返回错误让它改正，而不是中断循环。
		return {
			kind: "immediate",
			result: createErrorToolResult(`Tool ${toolCall.name} not found`),
			isError: true,
		};
	}

	try {
		// prepareArguments 在校验之前运行：模型给的原始参数格式可能不被 schema 接受，
		// 工具可以在这里做兼容转换。
		const preparedToolCall = prepareToolCallArguments(tool, toolCall);
		// 校验会做类型转换；之后的钩子和 execute 都拿转换后的值。
		const validatedArgs = validateToolArguments(tool, preparedToolCall);
		if (config.beforeToolCall) {
			// 常见用途是权限确认。钩子拿到 abort signal，需自行处理取消。
			const beforeResult = await config.beforeToolCall(
				{
					assistantMessage,
					toolCall,
					args: validatedArgs,
					context: currentContext,
				},
				signal,
			);
			// await 之后必须重新检查：等待期间可能被取消（await 本身不可取消）。
			if (signal?.aborted) {
				return {
					kind: "immediate",
					result: createErrorToolResult("Operation aborted"),
					isError: true,
				};
			}
			if (beforeResult?.block) {
				const result = createErrorToolResult(beforeResult.reason || "Tool execution was blocked");
				// 拦截结果仍是错误工具结果；terminate 透传，参与整批提前终止的判断。
				if (beforeResult.terminate === true) {
					result.terminate = true;
				}
				return {
					kind: "immediate",
					result,
					isError: true,
				};
			}
		}
		// 钩子返回后再检查一次，覆盖钩子内部触发取消的情况。
		if (signal?.aborted) {
			return {
				kind: "immediate",
				result: createErrorToolResult("Operation aborted"),
				isError: true,
			};
		}
		return {
			kind: "prepared",
			toolCall,
			tool,
			args: validatedArgs,
		};
	} catch (error) {
		// 校验或 prepareArguments 抛错：转成立即错误结果，把错误文本交给模型自行修正。
		return {
			kind: "immediate",
			result: createErrorToolResult(error instanceof Error ? error.message : String(error)),
			isError: true,
		};
	}
}

/** 把工具的 `onUpdate` 回调包装成 `tool_execution_update` 事件。 */
function emitToolExecutionUpdate(toolCall: AgentToolCall, emit: AgentEventSink): ToolUpdateSink {
	return (partialResult) =>
		emit({
			type: "tool_execution_update",
			toolCallId: toolCall.id,
			toolName: toolCall.name,
			args: toolCall.arguments,
			partialResult,
		});
}

/** {@link runToolCall} 的选项。 */
export interface RunToolCallOptions extends ToolCallHooks {
	/** 调用解析所依据的工具列表。 */
	tools: readonly AgentTool<any>[];
	/** 作为“发起该调用的消息”传给钩子。 */
	assistantMessage: AssistantMessage;
	/** 作为当前上下文传给钩子。 */
	context: AgentContext;
	signal?: AbortSignal;
	/** 工具流式中间结果的出口。 */
	onUpdate?: ToolUpdateSink;
}

/**
 * 执行单个工具调用，步骤与模型发起的调用完全一致：参数修正、schema 校验、
 * `beforeToolCall`、执行、`afterToolCall`。不发事件、不写消息。
 *
 * 用途：工具内部再调用其他工具（例如 codemode）时，让权限检查等钩子同样生效。
 * 永远不因工具失败而 reject：未知工具、校验失败、被拦截、执行抛错都返回 `isError: true`。
 */
export async function runToolCall(toolCall: AgentToolCall, options: RunToolCallOptions): Promise<AgentToolCallOutcome> {
	const { assistantMessage, context, signal } = options;
	const preparation = await prepareToolCall(context, assistantMessage, toolCall, options, signal, options.tools);
	if (preparation.kind === "immediate") {
		// 预检失败时直接返回，不执行。
		return { toolCall, result: preparation.result, isError: preparation.isError };
	}
	// 没有流式消费方时给一个空实现，保持 execute 的调用形状不变。
	const executed = await executePreparedToolCall(preparation, signal, options.onUpdate ?? (() => {}));
	return finalizeExecutedToolCall(context, assistantMessage, preparation, executed, options, signal);
}

/**
 * 实际执行已预检的工具，并收集它在执行期间发出的中间结果。
 *
 * `onUpdate` 可能返回 Promise：这里先攒起来而不是 await，工具自身的执行节奏不应被
 * 事件投递阻塞；但返回前会统一 await，保证 `tool_execution_update` 一定先于
 * `tool_execution_end` 到达。
 *
 * 执行抛错会被转成错误结果（而不是继续抛出），并同样记录耗时。
 */
async function executePreparedToolCall(
	prepared: PreparedToolCall,
	signal: AbortSignal | undefined,
	onUpdate: ToolUpdateSink,
): Promise<ExecutedToolCallOutcome> {
	const updateEvents: Promise<void>[] = [];
	let acceptingUpdates = true;
	// 使用单调时钟计时，避免系统时间调整导致负值。
	const startedAt = performance.now();
	const elapsed = () => Math.round(performance.now() - startedAt);

	try {
		const result = await prepared.tool.execute(
			prepared.toolCall.id,
			prepared.args as never,
			signal,
			(partialResult) => {
				// execute 结束后的迟到更新直接丢弃：契约规定回调只在本次执行内有效。
				if (!acceptingUpdates) return;
				// 包装成 Promise 后统一等待，兼容同步和异步的 onUpdate。
				updateEvents.push(Promise.resolve(onUpdate(partialResult)));
			},
		);
		const durationMs = elapsed();
		// 先停止接收，再等待已排队的更新，确保更新事件在结束事件之前全部送达。
		acceptingUpdates = false;
		await Promise.all(updateEvents);
		return { result, isError: result.isError === true, durationMs };
	} catch (error) {
		// 抛错路径同样要等完已排队的更新，否则结束事件可能先于更新事件。
		const durationMs = elapsed();
		acceptingUpdates = false;
		await Promise.all(updateEvents);
		return {
			result: createErrorToolResult(error instanceof Error ? error.message : String(error)),
			isError: true,
			durationMs,
		};
	} finally {
		// 无论成功、抛错还是提前返回，都关闭更新入口。
		acceptingUpdates = false;
	}
}

/**
 * 执行 `afterToolCall`，得到最终可对外发布的结果。
 *
 * 合并语义是逐字段替换（见 `AfterToolCallResult`）：钩子只返回想改的字段，其余保持执行结果。
 * `structuredContent` 单独处理：如果钩子换了 `content` 却没给新的结构化结果，旧的结构化
 * 结果可能已与 content 不一致，必须丢弃。钩子抛错时整个结果变成错误，避免半更新状态流出去。
 */
async function finalizeExecutedToolCall(
	currentContext: AgentContext,
	assistantMessage: AssistantMessage,
	prepared: PreparedToolCall,
	executed: ExecutedToolCallOutcome,
	config: ToolCallHooks,
	signal: AbortSignal | undefined,
): Promise<FinalizedToolCallOutcome> {
	let result = executed.result;
	let isError = executed.isError;

	if (config.afterToolCall) {
		try {
			const afterResult = await config.afterToolCall(
				{
					assistantMessage,
					toolCall: prepared.toolCall,
					args: prepared.args,
					result,
					isError,
					context: currentContext,
				},
				signal,
			);
			if (afterResult) {
				// content 与 structuredContent 是同一份数据的两种视图，要换就一起换。
				const structuredContent =
					afterResult.structuredContent ?? (afterResult.content ? undefined : result.structuredContent);
				result = {
					...result,
					content: afterResult.content ?? result.content,
					details: afterResult.details ?? result.details,
					usage: afterResult.usage ?? result.usage,
					terminate: afterResult.terminate ?? result.terminate,
				};
				// 用 delete 而不是赋 undefined：structuredContent 要么存在且合法，要么不存在；
				// 留着 undefined 的 key 容易让消费方误判为“声明了但为空”。
				if (structuredContent === undefined) delete result.structuredContent;
				else result.structuredContent = structuredContent;
				// 钩子可以翻转错误状态（例如把特定失败改成成功展示）。
				isError = afterResult.isError ?? isError;
			}
		} catch (error) {
			// 钩子异常不能让循环崩掉；换成错误结果，模型仍能继续。
			result = createErrorToolResult(error instanceof Error ? error.message : String(error));
			isError = true;
		}
	}

	return {
		toolCall: prepared.toolCall,
		result,
		isError,
		durationMs: executed.durationMs,
	};
}

/** 构造一个最小的错误工具结果：内容给模型看，details 保持空对象以满足类型。 */
function createErrorToolResult(message: string): AgentToolResult<any> {
	return {
		content: [{ type: "text", text: message }],
		details: {},
	};
}

/**
 * 发出工具执行结束事件。
 * `durationMs` 只在工具真正执行过时存在（未知工具/校验失败没有耗时），
 * 因此缺省时不写字段，避免 UI 显示无意义的 0ms。
 */
async function emitToolExecutionEnd(finalized: FinalizedToolCallOutcome, emit: AgentEventSink): Promise<void> {
	await emit({
		type: "tool_execution_end",
		toolCallId: finalized.toolCall.id,
		toolName: finalized.toolCall.name,
		result: finalized.result,
		isError: finalized.isError,
		...(finalized.durationMs === undefined ? {} : { durationMs: finalized.durationMs }),
	});
}

/**
 * 把最终结果转成 transcript 里的 toolResult 消息。
 * - `content` 兜底成空数组：JS 扩展等无类型工具可能返回空值，不能让它进入会话历史；
 * - `usage` 与 `durationMs` 一并带上，用于统计和 UI；
 * - `timestamp` 在这里生成，保证消息历史顺序稳定。
 */
function createToolResultMessage(finalized: FinalizedToolCallOutcome): ToolResultMessage {
	return {
		role: "toolResult",
		toolCallId: finalized.toolCall.id,
		toolName: finalized.toolCall.name,
		// 无类型工具（JS 扩展）可能不返回 content；规范成空数组再落历史。
		content: finalized.result.content ?? [],
		details: finalized.result.details,
		usage: finalized.result.usage,
		isError: finalized.isError,
		...(finalized.durationMs === undefined ? {} : { durationMs: finalized.durationMs }),
		timestamp: Date.now(),
	};
}

/** 工具结果消息也走标准消息生命周期：`message_start` 与 `message_end` 成对出现。 */
async function emitToolResultMessage(toolResultMessage: ToolResultMessage, emit: AgentEventSink): Promise<void> {
	await emit({ type: "message_start", message: toolResultMessage });
	await emit({ type: "message_end", message: toolResultMessage });
}
