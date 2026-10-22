/**
 * `read` 工具：读取文件内容，支持文本与图片。
 *
 * 分层说明：
 * - `createReadToolDefinition()` 返回 coding-agent 内部的 `ToolDefinition`，它比 agent-core 的
 *   `AgentTool` 多出系统提示词片段、输出 schema、渲染器等交互字段，这些只有 coding-agent 关心。
 * - 终端渲染逻辑拆到 `renderers/read.ts`：只负责显示工具输出的进程不必加载执行路径和参数 schema。
 * - `createReadTool()` 在最后把 `ToolDefinition` 降级成 `AgentTool`，交给 agent-core 的循环执行。
 */

import type { AgentTool } from "@earendil-works/pi-agent-core";
import type { Api, ImageContent, Model, ModelImageResizeOptions, TextContent } from "@earendil-works/pi-ai";
import { constants } from "fs";
import { access as fsAccess, readFile as fsReadFile } from "fs/promises";
import { type Static, Type } from "typebox";
// processImage 先做格式归一化（不支持的格式经 photon 转成 PNG），再按模型声明的体积上限缩放；
// 失败时返回一条文本说明而不是抛错，让调用方仍能拿到"读取成功但图片不可用"的结果。
import { processImage } from "../../utils/image-process.ts";
// 通过文件头魔数判断 MIME，而不是看扩展名，避免扩展名与内容不符时误判。
import { detectSupportedImageMimeTypeFromFile } from "../../utils/mime.ts";
import type { ExtensionContext, ToolDefinition } from "../extensions/types.ts";
import { resolveReadPathAsync } from "./path-utils.ts";
import { readRenderers } from "./renderers/read.ts";
import { wrapToolDefinition } from "./tool-definition-wrapper.ts";
import { DEFAULT_MAX_BYTES, DEFAULT_MAX_LINES, formatSize, type TruncationResult, truncateHead } from "./truncate.ts";

/**
 * 模型可见的参数 schema（TypeBox）。
 *
 * 只有 3 个字段；工作目录不在这里，而是由 `createReadToolDefinition(cwd)` 闭包捕获，
 * 运行时还会被 `ctx.cwd` 覆盖（见 execute 内的路径解析），这样对话中途切换 cwd 也能生效。
 * `offset` 是 1-indexed，与常见的行号显示习惯一致，进入实现后再转成数组下标。
 */
const readSchema = Type.Object({
	path: Type.String({ description: "Path to the file to read (relative or absolute)" }),
	offset: Type.Optional(Type.Number({ description: "Line number to start reading from (1-indexed)" })),
	limit: Type.Optional(Type.Number({ description: "Maximum number of lines to read" })),
});

/**
 * 该工具对系统提示词的贡献：一行能力摘要 + 一条使用准则。
 *
 * 这里只放数据，不拼字符串；真正的拼接由 system-prompt 在工具激活时完成：
 * 片段进 "Available tools" 列表，准则进 "Guidelines" 列表（system-prompt.ts:118 与 :159）。
 * 拆开是为了让未激活的工具不污染提示词。
 */
export const readToolSystemPromptContribution = {
	snippet: "Read file contents",
	guidelines: ["Use read to examine files instead of cat or sed."],
} as const;

export type ReadToolInput = Static<typeof readSchema>;

/**
 * 结构化输出 schema，面向 codemode 脚本这类程序化调用方（不是给模型看的）。
 *
 * 文本文件返回字符串；图片返回一个带 base64 data 的对象，codemode 的 `image()` 可以直接消费。
 * 这里的 property description 刻意省略，让该类型在工具描述里保持单行，避免撑大提示词。
 */
const readOutputSchema = Type.Union([
	Type.String(),
	Type.Object({ type: Type.Literal("image"), data: Type.String(), mimeType: Type.String(), note: Type.String() }),
]);

export type ReadToolOutput = Static<typeof readOutputSchema>;

/**
 * 返回给 UI 的附加元数据（不发送给模型）。
 *
 * `truncation` 让渲染器能画出 "[Truncated: showing 2000 of 5000 lines]" 这类提示。
 * 之所以不塞进 `content`，是因为模型已经拿到了可续读的偏移量提示，UI 需要的是另一套措辞。
 */
export interface ReadToolDetails {
	truncation?: TruncationResult;
}

/**
 * 可替换的文件操作层。
 *
 * 默认实现直接走本地文件系统；把它抽出来是为了让工具能被远程环境复用，
 * 例如通过 SSH 把读文件、探测 MIME 委托给远端主机。
 */
export interface ReadOperations {
	/** 读取文件原始字节 */
	readFile: (absolutePath: string) => Promise<Buffer>;
	/** 检查文件存在且可读，不可读时抛错 */
	access: (absolutePath: string) => Promise<void>;
	/** 通过内容判断图片 MIME；不是图片时返回 null 或 undefined */
	detectImageMimeType?: (absolutePath: string) => Promise<string | null | undefined>;
}

/** 本地文件系统的默认实现：全部是 fs 模块的薄封装。 */
const defaultReadOperations: ReadOperations = {
	readFile: (path) => fsReadFile(path),
	access: (path) => fsAccess(path, constants.R_OK),
	detectImageMimeType: detectSupportedImageMimeTypeFromFile,
};

/** 构造参数。所有字段都可选，默认值在 `createReadToolDefinition` 内解析。 */
export interface ReadToolOptions {
	/** 是否自动缩放图片。默认 true */
	autoResizeImages?: boolean;
	/** 执行上下文里没有模型元数据时使用的兜底缩放参数 */
	resizeOptions?: ModelImageResizeOptions;
	/** 自定义文件操作。默认本地文件系统 */
	operations?: ReadOperations;
}

/**
 * 把模型面向的 content 块转成程序化调用方面向的 `structuredContent`。
 *
 * 规则：图片块存在时返回图片对象（文本块作为 note 附带，通常是尺寸/缩放提示），
 * 否则返回纯文本。两者取不到时退化成空字符串，保证返回值始终符合 schema。
 */
function toReadOutput(content: (TextContent | ImageContent)[]): ReadToolOutput {
	const text = content.find((block) => block.type === "text")?.text ?? "";
	const image = content.find((block) => block.type === "image");
	return image ? { type: "image", data: image.data, mimeType: image.mimeType, note: text } : text;
}

/**
 * 当前模型不支持图片输入时返回一条说明，否则返回 undefined。
 *
 * 之所以仍然把图片读出来再附一句提示，而不是直接拒绝：用户可能在会话中途切到纯文本模型，
 * 保留文件读取成功的事实比报错更有用，提示也会随结果一起进上下文。
 */
function getNonVisionImageNote(model: Model<Api> | undefined): string | undefined {
	if (!model || model.input.includes("image")) {
		return undefined;
	}
	return "[Current model does not support images. The image will be omitted from this request.]";
}

/**
 * 构造 `read` 的工具定义。
 *
 * @param cwd 闭包内保存的默认工作目录，运行时会被 `ctx.cwd` 覆盖
 * @param options 图片缩放与文件操作的覆盖项
 */
export function createReadToolDefinition(
	cwd: string,
	options?: ReadToolOptions,
): ToolDefinition<typeof readSchema, ReadToolDetails | undefined> {
	// 在构造期解析一次默认值，避免每次调用都做 ?? 判断。
	const autoResizeImages = options?.autoResizeImages ?? true;
	const fallbackResizeOptions = options?.resizeOptions;
	const ops = options?.operations ?? defaultReadOperations;
	return {
		name: "read",
		label: "read",
		// 描述里写死了截断阈值，让模型在调用前就知道需要配合 offset 续读。
		description: `Read the contents of a file. Supports text files and images (jpg, png, gif, webp, bmp). Images are sent as attachments. For text files, output is truncated to ${DEFAULT_MAX_LINES} lines or ${DEFAULT_MAX_BYTES / 1024}KB (whichever is hit first). Use offset/limit for large files. When you need the full file, continue with offset until complete.`,
		promptSnippet: readToolSystemPromptContribution.snippet,
		promptGuidelines: [...readToolSystemPromptContribution.guidelines],
		parameters: readSchema,
		outputSchema: readOutputSchema,
		// 支持约束采样的 provider 会按 outputSchema 强制参数格式；不支持的 provider 退化为普通提示，
		// 因此这里是 "prefer" 而非 "require"，避免在不兼容的模型上报错。
		constrainedSampling: { type: "json_schema", strict: "prefer" },
		async execute(
			_toolCallId,
			{ path, offset, limit }: { path: string; offset?: number; limit?: number },
			signal?: AbortSignal,
			// read 不产生流式中间结果，因此忽略 onUpdate。
			_onUpdate?,
			ctx?: ExtensionContext,
		) {
			// 外层用 new Promise 包一层，是为了在调用返回前就同步注册 abort 监听，
			// 并在每个 await 之后检查 aborted 标记（await 本身不可取消）。
			return new Promise<{ content: (TextContent | ImageContent)[]; details: ReadToolDetails | undefined }>(
				(resolve, reject) => {
					// 若调用前就已取消，直接失败，不触碰文件系统。
					if (signal?.aborted) {
						reject(new Error("Operation aborted"));
						return;
					}
					let aborted = false;
					const onAbort = () => {
						aborted = true;
						reject(new Error("Operation aborted"));
					};
					signal?.addEventListener("abort", onAbort, { once: true });

					// 内层 async IIFE 承载真正的流程；它返回的 Promise 不参与外层决议，
					// 外层只由 resolve/reject 决定，因此取消后内层剩余步骤会被 aborted 检查拦截。
					(async () => {
						try {
							// ctx.cwd 优先：会话中途切换目录时，相对路径要按新目录解析。
							const absolutePath = await resolveReadPathAsync(path, ctx?.cwd || cwd);
							if (aborted) return;
							// 先确认可读，让权限错误/文件不存在尽早暴露，而不是在读取时才发现。
							await ops.access(absolutePath);
							if (aborted) return;
							const mimeType = ops.detectImageMimeType ? await ops.detectImageMimeType(absolutePath) : undefined;
							let content: (TextContent | ImageContent)[];
							let details: ReadToolDetails | undefined;
							const nonVisionImageNote = getNonVisionImageNote(ctx?.model);
							if (mimeType) {
								// 图片分支：读出原始字节，交给 processImage 做体积/尺寸归一化。
								// 缩放参数优先取当前模型声明的图片限制，其次退回构造期传入的兜底值。
								const buffer = await ops.readFile(absolutePath);
								const processed = await processImage(buffer, mimeType, {
									autoResizeImages,
									resizeOptions: ctx?.model?.inputLimits?.images?.resize ?? fallbackResizeOptions,
								});
								if (!processed.ok) {
									// 处理失败（解码失败或缩放后仍超限）时退化成文本说明，保证调用本身不失败。
									let textNote = `Read image file [${mimeType}]\n${processed.message}`;
									if (nonVisionImageNote) textNote += `\n${nonVisionImageNote}`;
									content = [{ type: "text", text: textNote }];
								} else {
									// 成功时同时返回文本说明（含缩放提示）和图片块，模型两者都能看到。
									let textNote = `Read image file [${processed.mimeType}]`;
									if (processed.hints.length > 0) textNote += `\n${processed.hints.join("\n")}`;
									if (nonVisionImageNote) textNote += `\n${nonVisionImageNote}`;
									content = [
										{ type: "text", text: textNote },
										{ type: "image", data: processed.data, mimeType: processed.mimeType },
									];
								}
							} else {
								// 文本分支：整文件读入后 split，用行数组同时支持 offset 与行数统计。
								const buffer = await ops.readFile(absolutePath);
								const textContent = buffer.toString("utf-8");
								const allLines = textContent.split("\n");
								const totalFileLines = allLines.length;
								// offset 是 1-indexed，转成 0-indexed 下标；offset 为 0 或负数时钳制到文件开头。
								const startLine = offset ? Math.max(0, offset - 1) : 0;
								const startLineDisplay = startLine + 1;
								// 越界直接报错，让模型知道该换更小的 offset，而不是拿到空结果困惑。
								if (startLine >= allLines.length) {
									throw new Error(`Offset ${offset} is beyond end of file (${allLines.length} lines total)`);
								}
								let selectedContent: string;
								let userLimitedLines: number | undefined;
								// 用户显式 limit 优先；否则先切片到文件末尾，剩下的交给 truncateHead 统一裁决。
								if (limit !== undefined) {
									const endLine = Math.min(startLine + limit, allLines.length);
									selectedContent = allLines.slice(startLine, endLine).join("\n");
									userLimitedLines = endLine - startLine;
								} else {
									selectedContent = allLines.slice(startLine).join("\n");
								}
								// 统一按 2000 行 / 50KB 两个维度截断，先命中的生效。
								const truncation = truncateHead(selectedContent);
								let outputText: string;
								if (truncation.firstLineExceedsLimit) {
									// 单行就超过字节上限：返回任何内容都会再次超限，于是不返回正文，
									// 而是给出一条可以直接复制的 bash 命令作为兜底。
									const firstLineSize = formatSize(Buffer.byteLength(allLines[startLine], "utf-8"));
									outputText = `[Line ${startLineDisplay} is ${firstLineSize}, exceeds ${formatSize(DEFAULT_MAX_BYTES)} limit. Use bash: sed -n '${startLineDisplay}p' ${path} | head -c ${DEFAULT_MAX_BYTES}]`;
									details = { truncation };
								} else if (truncation.truncated) {
									// 发生截断：在正文后追加可操作的续读提示，明确给出下一段 offset。
									const endLineDisplay = startLineDisplay + truncation.outputLines - 1;
									const nextOffset = endLineDisplay + 1;
									outputText = truncation.content;
									if (truncation.truncatedBy === "lines") {
										outputText += `\n\n[Showing lines ${startLineDisplay}-${endLineDisplay} of ${totalFileLines}. Use offset=${nextOffset} to continue.]`;
									} else {
										outputText += `\n\n[Showing lines ${startLineDisplay}-${endLineDisplay} of ${totalFileLines} (${formatSize(DEFAULT_MAX_BYTES)} limit). Use offset=${nextOffset} to continue.]`;
									}
									details = { truncation };
								} else if (userLimitedLines !== undefined && startLine + userLimitedLines < allLines.length) {
									// 没有触发硬截断，但用户给的 limit 提前停在文件中间：仍提示剩余行数。
									const remaining = allLines.length - (startLine + userLimitedLines);
									const nextOffset = startLine + userLimitedLines + 1;
									outputText = `${truncation.content}\n\n[${remaining} more lines in file. Use offset=${nextOffset} to continue.]`;
								} else {
									// 完整读完到文件尾，原样返回。
									outputText = truncation.content;
								}
								content = [{ type: "text", text: outputText }];
							}

							if (aborted) return;
							// 正常结束才移除监听，避免长时间会话里 signal 上堆积闭包。
							signal?.removeEventListener("abort", onAbort);
							resolve({ content, details });
						} catch (error: any) {
							signal?.removeEventListener("abort", onAbort);
							// 取消导致的 reject 已由 onAbort 完成，这里吞掉错误避免 unhandled rejection。
							if (!aborted) reject(error);
						}
					})();
				},
				// 在 AgentToolResult 之外补上 structuredContent：模型侧仍看 content，
				// codemode 之类的程序化调用方拿结构化结果。
			).then((result) => ({ ...result, structuredContent: toReadOutput(result.content) }));
		},
		// 展开/折叠、语法高亮、图片展示等终端表现，全部来自独立文件。
		...readRenderers,
	};
}

/**
 * 把工具定义降级为 agent-core 的 `AgentTool`。
 *
 * 降级过程中 promptSnippet、promptGuidelines、outputSchema、renderers、exposure 等
 * coding-agent 专属字段都会丢失，因为 agent-core 的循环只需要名字、参数 schema 和 execute。
 */
export function createReadTool(cwd: string, options?: ReadToolOptions): AgentTool<typeof readSchema> {
	return wrapToolDefinition(createReadToolDefinition(cwd, options));
}
