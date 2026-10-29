import { fileURLToPath } from "node:url";
import { createActivityObservabilityInterceptors } from "./activity-observability.js";
import type { InjectedSinks } from "@temporalio/worker";
import { createOtlpWorkflowSinks, type OtlpWorkflowSinks } from "./otlp-workflow-sink.js";
import type { OtlpSpan, OtlpSpanLink } from "./otlp-attributes.js";

export interface ServerSpan {
  traceId: string;
  spanId: string;
  parentSpanId: string | null;
  name: string;
  startTimeUnixNano: bigint;
  endTimeUnixNano: bigint;
  attributes: Readonly<Record<string, string | number>>;
  error: boolean;
  kind?: number;
  links?: readonly OtlpSpanLink[];
}

export interface SpanExporterStats {
  exported: number;
  dropped: number;
  failed: number;
}

export interface SpanExporter {
  record(span: ServerSpan): void;
  flush(): Promise<void>;
  close(): Promise<void>;
  stats(): SpanExporterStats;
}

export interface OtlpSpanExporterOptions {
  endpoint: string;
  serviceName: string;
  fetch?: (url: string, init: { method: string; headers: Record<string, string>; body: string; signal: AbortSignal }) => Promise<{ ok: boolean }>;
  batchSize?: number;
  maxQueue?: number;
  flushIntervalMs?: number;
  timeoutMs?: number;
}

export interface WorkerOtlpConfiguration {
  spanExporter: SpanExporter | null;
  sinks: InjectedSinks<OtlpWorkflowSinks>;
  interceptors: {
    workflowModules: string[];
    activity: ReturnType<typeof createActivityObservabilityInterceptors>[];
  };
}

export function otlpTracesUrl(value: string | undefined): string | null {
  if (!value?.trim()) return null;
  let url: URL;
  try {
    url = new URL(value.trim());
  } catch {
    return null;
  }
  if ((url.protocol !== "http:" && url.protocol !== "https:") || url.username || url.password || url.search || url.hash) return null;
  return `${url.origin}${url.pathname.replace(/\/+$/, "")}/v1/traces`;
}

function attributeValue(value: unknown) {
  return typeof value === "number" && Number.isInteger(value) ? { intValue: String(value) } : { stringValue: String(value) };
}

export function workerOtlpTraceRequest(spans: readonly OtlpSpan[], serviceName: string) {
  return {
    resourceSpans: [{
      resource: { attributes: [{ key: "service.name", value: { stringValue: serviceName } }] },
      scopeSpans: [{
        scope: { name: serviceName },
        spans: spans.map(span => ({
          traceId: span.traceId,
          spanId: span.spanId,
          ...(span.parentSpanId ? { parentSpanId: span.parentSpanId } : {}),
          name: span.name,
          kind: span.kind ?? 2,
          startTimeUnixNano: span.startTimeUnixNano.toString(),
          endTimeUnixNano: span.endTimeUnixNano.toString(),
          attributes: Object.entries(span.attributes).map(([key, value]) => ({ key, value: attributeValue(value) })),
          status: { code: span.error ? 2 : 0 },
          ...(span.links?.length ? {
            links: span.links.map(l => ({
              traceId: l.traceId,
              spanId: l.spanId,
              ...(l.attributes ? { attributes: Object.entries(l.attributes).map(([k, v]) => ({ key: k, value: attributeValue(v) })) } : {})
            }))
          } : {})
        }))
      }]
    }]
  };
}

const hexId = (length: number) => new RegExp(`^[0-9a-f]{${length}}$`);
const traceIdPattern = hexId(32);
const spanIdPattern = hexId(16);

export function createOtlpSpanExporter(options: OtlpSpanExporterOptions): SpanExporter {
  const url = otlpTracesUrl(options.endpoint);
  if (!url) throw new Error("Invalid OTLP endpoint.");
  const send = options.fetch ?? ((target: string, init: any) => fetch(target, init));
  const batchSize = options.batchSize ?? 256;
  const maxQueue = options.maxQueue ?? 4096;
  const timeoutMs = options.timeoutMs ?? 3000;
  const queue: OtlpSpan[] = [];
  const stats: SpanExporterStats = { exported: 0, dropped: 0, failed: 0 };
  let inFlight: Promise<void> | null = null;

  const flushOnce = async () => {
    const batch = queue.splice(0, batchSize);
    if (!batch.length) return;
    try {
      const response = await send(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(workerOtlpTraceRequest(batch, options.serviceName)),
        signal: AbortSignal.timeout(timeoutMs)
      });
      if (response.ok) stats.exported += batch.length;
      else stats.failed += batch.length;
    } catch {
      stats.failed += batch.length;
    }
  };

  const flush = (): Promise<void> => {
    if (!inFlight) {
      inFlight = (async () => {
        try {
          while (queue.length) await flushOnce();
        } finally {
          inFlight = null;
        }
      })();
    }
    return inFlight;
  };

  const timer = setInterval(() => void flush(), options.flushIntervalMs ?? 5000);
  timer.unref?.();

  return {
    record(span: ServerSpan) {
      if (!traceIdPattern.test(span.traceId) || !spanIdPattern.test(span.spanId)) return;
      if (queue.length >= maxQueue) {
        stats.dropped += 1;
        return;
      }
      queue.push(span as OtlpSpan);
      if (queue.length >= batchSize) void flush();
    },
    flush,
    async close() {
      clearInterval(timer);
      await flush();
    },
    stats: () => ({ ...stats })
  };
}

export function otlpSpanExporterFromEnv(env: NodeJS.ProcessEnv, serviceName: string): SpanExporter | null {
  const endpoint = env.SPACE_OTEL_EXPORTER_OTLP_ENDPOINT;
  return otlpTracesUrl(endpoint) ? createOtlpSpanExporter({ endpoint: endpoint!, serviceName }) : null;
}

export function configureWorkerOtlp(serviceName: string, env: NodeJS.ProcessEnv = process.env): WorkerOtlpConfiguration {
  const spanExporter = otlpSpanExporterFromEnv(env, serviceName);
  const sinks = createOtlpWorkflowSinks(spanExporter);
  const workflowInterceptorsPath = fileURLToPath(new URL("./otlp-workflow-interceptors.js", import.meta.url));

  return {
    spanExporter,
    sinks,
    interceptors: {
      workflowModules: [workflowInterceptorsPath],
      activity: [createActivityObservabilityInterceptors({ spanExporter })]
    }
  };
}
