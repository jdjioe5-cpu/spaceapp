import { AsyncLocalStorage } from "node:async_hooks";
import { randomBytes } from "node:crypto";
import type { Context } from "@temporalio/activity";
import type { ActivityInterceptorsFactory } from "@temporalio/worker";
import {
  categorizeFailureCode,
  formatTraceparent,
  isSafeErrorCode,
  parseTraceparent,
  traceIdFromTraceparent,
  type FailureCategory
} from "@space/contracts";
import type { SpanExporter } from "@space/runtime";
import { logIdentifier, workerLog, type WorkerLogValue } from "./worker-log.js";
import { workerMetrics, type WorkerMetrics } from "./worker-metrics.js";
import { allowlistSpanAttributes, runIdToSpanId, type OtlpSpan, type OtlpSpanLink } from "./otlp-attributes.js";

/**
 * One structured start/finish record and metrics per activity execution, with
 * the correlation ids the API put into the workflow input. Runs in the worker
 * process (activity interceptor), never inside the deterministic workflow
 * sandbox, so it cannot affect replay.
 */

type ActivityInfo = Pick<
  Context["info"],
  "activityType" | "attempt" | "workflowExecution" | "workflowType" | "currentAttemptScheduledTimestampMs" | "activityId"
>;

export type ActivityOutcome = "COMPLETED" | "FAILED_RESULT" | "FAILED" | "CANCELLED";

interface ActivityObservation {
  reasonCode: string | null;
}

const observations = new AsyncLocalStorage<ActivityObservation>();

/**
 * Called by failure recorders (for example recordCodexAppServerTurnFailure)
 * that return a FAILED result instead of throwing, so the activity record
 * carries the causal reason code. A no-op outside an observed activity.
 */
export function noteActivityFailureReason(reasonCode: string): void {
  const observation = observations.getStore();
  if (observation && isSafeErrorCode(reasonCode)) observation.reasonCode = reasonCode;
}

/**
 * Runs `execute` inside a failure-reason scope, reusing the activity's scope
 * when the interceptor already opened one, and exposes the noted reason code.
 */
export function failureReasonScope<T>(execute: () => Promise<T>): { promise: Promise<T>; reasonCode: () => string | null } {
  const existing = observations.getStore();
  if (existing) return { promise: execute(), reasonCode: () => existing.reasonCode };
  const observation: ActivityObservation = { reasonCode: null };
  return { promise: observations.run(observation, execute), reasonCode: () => observation.reasonCode };
}

/** traceparent for an outbound call: same W3C trace, new span id; null if none. */
export function childTraceparent(traceparent: string | null | undefined): string | null {
  const parsed = parseTraceparent(traceparent);
  if (!parsed) return null;
  let spanId = randomBytes(8).toString("hex");
  while (/^0+$/.test(spanId)) spanId = randomBytes(8).toString("hex");
  return formatTraceparent(parsed.traceId, spanId, parsed.flags);
}

// Keys whose values are identifiers. Prompts, content and messages are never read.
const correlationKeys = [
  "traceId",
  "traceparent",
  "requestId",
  "roomId",
  "paneId",
  "missionId",
  "roomAgentMissionId",
  "agentRunId",
  "agentSessionId",
  "actionId",
  "goalId",
  "dotId",
  "turnId",
  "cliTaskId",
  "revisionId"
] as const;
const nestedKeys = ["turn", "item", "bridge", "input", "turnInput"] as const;

type CorrelationKey = (typeof correlationKeys)[number];

function readCorrelation(value: unknown, into: Partial<Record<CorrelationKey, string>>, depth: number): void {
  if (!value || typeof value !== "object" || Array.isArray(value)) return;
  const record = value as Record<string, unknown>;
  for (const key of correlationKeys) {
    if (into[key] !== undefined) continue;
    const identifier = logIdentifier(record[key]);
    if (identifier !== null) into[key] = identifier;
  }
  if (depth <= 0) return;
  for (const key of nestedKeys) readCorrelation(record[key], into, depth - 1);
}

/** Allowlisted correlation fields from activity arguments (identifiers only). */
export function activityCorrelation(args: readonly unknown[]): Record<string, string> {
  const found: Partial<Record<CorrelationKey, string>> = {};
  for (const arg of args) readCorrelation(arg, found, 2);
  const correlation: Record<string, string> = {};
  if (found.traceId) correlation.traceId = found.traceId;
  const w3cTraceId = traceIdFromTraceparent(found.traceparent);
  if (w3cTraceId) correlation.w3cTraceId = w3cTraceId;
  if (found.requestId && found.requestId !== found.traceId) correlation.parentTraceId = found.requestId;
  const missionId = found.missionId ?? found.roomAgentMissionId;
  if (missionId) correlation.missionId = missionId;
  for (const key of ["roomId", "paneId", "agentRunId", "agentSessionId", "actionId", "goalId", "dotId", "turnId", "cliTaskId", "revisionId"] as const) {
    if (found[key]) correlation[key] = found[key]!;
  }
  return correlation;
}

function errorCodeOf(error: unknown): string {
  const record = error as { code?: unknown; type?: unknown; name?: unknown } | null;
  if (isSafeErrorCode(record?.code)) return record!.code as string;
  if (isSafeErrorCode(record?.type)) return record!.type as string;
  const name = typeof record?.name === "string" ? record.name : "";
  if (/Cancel/i.test(name)) return "ACTIVITY_CANCELLED";
  if (/Timeout/i.test(name)) return "ACTIVITY_TIMEOUT";
  if (/NotFound/i.test(name)) return "STATE_NOT_FOUND";
  if (/Retryable/i.test(name)) return "UPSTREAM_RETRYABLE_UNAVAILABLE";
  return "ACTIVITY_ERROR";
}

function isFailedResult(value: unknown): boolean {
  return Boolean(value && typeof value === "object" && (value as { status?: unknown }).status === "FAILED");
}

export interface ObserveActivityDependencies {
  spanExporter?: SpanExporter | null;
  metrics?: WorkerMetrics;
  now?: () => number;
  cancelled?: () => boolean;
  /** OTLP export (SPACE_OTEL_EXPORTER_OTLP_ENDPOINT): one span per activity whose input carries a traceparent. */
  spans?: SpanExporter | null;
}

function activityTraceparent(args: readonly unknown[]) {
  const found: Partial<Record<CorrelationKey, string>> = {};
  for (const arg of args) readCorrelation(arg, found, 2);
  return parseTraceparent(found.traceparent);
}

export async function observeActivityExecution<T>(
  info: ActivityInfo,
  args: readonly unknown[],
  execute: () => Promise<T>,
  dependencies: ObserveActivityDependencies = {}
): Promise<T> {
  const metrics = dependencies.metrics ?? workerMetrics;
  const now = dependencies.now ?? Date.now;
  const activityType = logIdentifier(info.activityType) ?? "unknown";
  const startedAt = now();
  const queueMs = Number.isFinite(info.currentAttemptScheduledTimestampMs)
    ? Math.max(0, startedAt - info.currentAttemptScheduledTimestampMs)
    : null;
  const base: Record<string, WorkerLogValue> = {
    activityType,
    activityId: info.activityId,
    workflowType: info.workflowType ?? null,
    workflowId: info.workflowExecution?.workflowId ?? null,
    temporalRunId: info.workflowExecution?.runId ?? null,
    attempt: info.attempt,
    ...activityCorrelation(args)
  };
  const labels = { activity_type: activityType };
  if (info.attempt > 1) metrics.activityRetries.inc(labels);
  if (queueMs !== null) metrics.activityScheduleToStart.observe(labels, queueMs / 1000);
  metrics.activitiesInFlight.inc(labels);
  workerLog("info", "activity.started", { ...base, queueMs });
  const observation: ActivityObservation = { reasonCode: null };

  const finish = (outcome: ActivityOutcome, reasonCode: string | null, errorName: string | null) => {
    const durationMs = Math.max(0, now() - startedAt);
    metrics.activitiesInFlight.inc(labels, -1);
    metrics.activityExecutions.inc({ activity_type: activityType, outcome });
    metrics.activityDuration.observe(labels, durationMs / 1000);
    let category: FailureCategory | null = null;
    if (outcome !== "COMPLETED") {
      category = outcome === "CANCELLED" ? "CANCELLED" : categorizeFailureCode(reasonCode);
      metrics.activityFailures.inc({ activity_type: activityType, category, code: reasonCode ?? "UNSPECIFIED" });
    }
    workerLog(outcome === "COMPLETED" ? "info" : outcome === "CANCELLED" ? "warn" : "error", "activity.finished", {
      ...base,
      outcome,
      reasonCode,
      category,
      errorName,
      durationMs,
      queueMs,
      rssBytes: process.memoryUsage().rss
    });
    const parent = dependencies.spans ? activityTraceparent(args) : null;
    if (parent) {
      dependencies.spans!.record({
        traceId: parent.traceId,
        spanId: randomBytes(8).toString("hex"),
        parentSpanId: parent.parentId,
        name: `activity ${activityType}`,
        startTimeUnixNano: BigInt(startedAt) * 1_000_000n,
        endTimeUnixNano: BigInt(startedAt + durationMs) * 1_000_000n,
        attributes: { "space.activity_type": activityType, "space.outcome": outcome, "space.attempt": info.attempt },
        error: outcome === "FAILED" || outcome === "FAILED_RESULT",
        kind: 1
      });
    }
  };

  try {
    const result = await observations.run(observation, execute);
    if (isFailedResult(result)) {
      const code = observation.reasonCode ?? "ACTIVITY_RESULT_FAILED";
      finish("FAILED_RESULT", code, null);
      if (dependencies.spanExporter) exportActivitySpanOtlp(dependencies, info, args, activityType, startedAt, now() - startedAt, queueMs, "FAILED_RESULT", categorizeFailureCode(code), code, null);
    } else {
      finish("COMPLETED", null, null);
      if (dependencies.spanExporter) exportActivitySpanOtlp(dependencies, info, args, activityType, startedAt, now() - startedAt, queueMs, "COMPLETED", null, null, null);
    }
    return result;
  } catch (error) {
    const cancelled = dependencies.cancelled?.() === true || /Cancel/i.test((error as { name?: string } | null)?.name ?? "");
    const errorName = logIdentifier((error as { name?: unknown } | null)?.name);
    const code = observation.reasonCode ?? errorCodeOf(error);
    const outcome: ActivityOutcome = cancelled ? "CANCELLED" : "FAILED";
    const cat = cancelled ? "CANCELLED" : categorizeFailureCode(code);
    finish(outcome, code, errorName);
    if (dependencies.spanExporter) exportActivitySpanOtlp(dependencies, info, args, activityType, startedAt, now() - startedAt, queueMs, outcome, cat, code, errorName);
    throw error;
  }
}

/** Temporal worker interceptor factory: register in Worker.create({ interceptors }). */
export function createActivityObservabilityInterceptors(dependencies: Omit<ObserveActivityDependencies, "cancelled"> = {}): ActivityInterceptorsFactory {
  return (context) => ({
    inbound: {
      execute: (input, next) => observeActivityExecution(context.info, input.args, () => next(input), {
        ...dependencies,
        cancelled: () => context.cancellationSignal.aborted
      })
    }
  });
}


function exportActivitySpanOtlp(
  dependencies: ObserveActivityDependencies,
  info: ActivityInfo,
  args: readonly unknown[],
  activityType: string,
  startedAt: number,
  durationMs: number,
  queueMs: number | null,
  outcome: ActivityOutcome,
  category: FailureCategory | null,
  reasonCode: string | null,
  errorName: string | null
) {
  const exporter = dependencies.spanExporter;
  if (!exporter) return;
  try {
    const correlation = activityCorrelation(args);
    const parent = activityTraceparent(args);
    let traceId: string | null = parent?.traceId ?? null;
    const temporalRunId = info.workflowExecution?.runId ?? null;
    let parentSpanId: string | null = temporalRunId ? runIdToSpanId(temporalRunId) : (parent?.parentId ?? null);

    if (!traceId && correlation.traceId && /^[0-9a-f]{32}$/i.test(correlation.traceId)) {
      traceId = correlation.traceId.toLowerCase();
      if (temporalRunId && !parentSpanId) {
        parentSpanId = runIdToSpanId(temporalRunId);
      }
    }
    if (!traceId) return;

    let spanId = randomBytes(8).toString("hex");
    while (/^0+$/.test(spanId)) spanId = randomBytes(8).toString("hex");

    const rawAttrs: Record<string, unknown> = {
      "service.name": "space-worker",
      "temporal.activity_type": activityType,
      "temporal.activity_id": info.activityId,
      "temporal.attempt": info.attempt,
      "temporal.workflow_type": info.workflowType ?? null,
      "temporal.workflow_id": info.workflowExecution?.workflowId ?? null,
      "temporal.run_id": temporalRunId,
      "temporal.status": outcome === "COMPLETED" ? "COMPLETED" : "FAILED",
      "temporal.outcome": outcome,
      "temporal.duration_ms": durationMs,
      "temporal.queue_ms": queueMs,
      "temporal.reason_code": reasonCode,
      "temporal.failure_category": category,
      "temporal.error_name": errorName,
      "space.room_id": correlation.roomId,
      "space.pane_id": correlation.paneId,
      "space.mission_id": correlation.missionId,
      "space.turn_id": correlation.turnId,
      "space.action_id": correlation.actionId,
      "space.agent_run_id": correlation.agentRunId,
      "space.agent_session_id": correlation.agentSessionId,
      "space.trace_id": correlation.traceId,
      "space.parent_trace_id": correlation.parentTraceId,
      "space.activity_type": activityType,
      "space.outcome": outcome,
      "space.attempt": info.attempt
    };
    const attributes = allowlistSpanAttributes(rawAttrs);

    const links: OtlpSpanLink[] = [];
    for (const arg of args) {
      if (!arg || typeof arg !== "object") continue;
      const r = arg as Record<string, unknown>;
      const turn = (r.turn ?? (r.item as Record<string, unknown>)?.turn) as Record<string, unknown> | undefined;
      const cont = (r.roomAgentContinuation ?? turn?.roomAgentContinuation) as { sourceRunId?: string } | undefined;
      if (cont?.sourceRunId) {
        links.push({
          traceId,
          spanId: runIdToSpanId(cont.sourceRunId),
          attributes: { "link.reason": "room_background_continuation" }
        });
        break;
      }
    }

    const span: OtlpSpan = {
      traceId,
      spanId,
      parentSpanId,
      name: `activity ${activityType}`,
      kind: 1,
      startTimeUnixNano: BigInt(startedAt) * 1_000_000n,
      endTimeUnixNano: BigInt(startedAt + durationMs) * 1_000_000n,
      attributes,
      error: outcome === "FAILED" || outcome === "FAILED_RESULT",
      links: links.length > 0 ? links : undefined
    };
    exporter.record(span);
  } catch {}
}
