# Claw Agent Flow Architecture

## Overview

Agent Flow is a redesign of the ESP-Claw core agent architecture, inspired by
Claude Code's query-loop + tool-orchestration design. It introduces explicit
state machine management, concurrent tool execution, and a pluggable context
pipeline while maintaining compatibility with existing `claw_core`,
`claw_event_router`, and `claw_cap` subsystems.

## Architecture Layers

```
+-------------------------------------------------------------+
|  Application Layer (app_claw.c)                             |
|    - agent_flow_init() / agent_flow_start()                 |
|    - Register context providers                             |
|    - Submit requests via event router                       |
+-------------------------------------------------------------+
|  Agent Orchestrator (claw_agent_flow.c)                     |
|    - FreeRTOS task + request/response queues                |
|    - Lifecycle: init -> start -> stop                       |
|    - In-flight request tracking & cancellation              |
+-------------------------------------------------------------+
|  Query Loop Engine (agent_flow_query_loop)                  |
|    - Per-request state machine reset                        |
|    - CONTEXT_BUILD -> LLM_CALL -> STREAMING                 |
|    -> TOOL_DISPATCH -> TOOL_EXECUTE -> OBSERVE -> loop      |
|    -> RESPONDING/DONE/ERROR/CANCELLED                       |
+-------------------------------------------------------------+
|  Tool Orchestrator (claw_agent_tool_orchestrator.c)         |
|    - Registry: register/unregister/find tools               |
|    - Batch executor with concurrency control                |
|      * CONC_SAFE   -> parallel FreeRTOS tasks               |
|      * CONC_EXCLUSIVE -> serial with mutex                  |
|    - Permission hook integration                            |
+-------------------------------------------------------------+
|  State Machine (claw_agent_state_machine.c)                 |
|    - Table-driven transitions (default + custom)            |
|    - Guards against invalid transitions                     |
|    - State change callback for observability                |
+-------------------------------------------------------------+
|  Context Pipeline (claw_agent_context_pipeline.c)           |
|    - Priority-sorted provider chain                         |
|    - Token budget tracking & estimation                     |
|    - Compaction hook when threshold exceeded                |
|    - Assembles: system_prompt + messages[] + tools[]        |
+-------------------------------------------------------------+
|  Integration Layer                                          |
|    - agent_flow_install_cap_bridge()                        |
|      Maps existing claw_cap descriptors to agent tools      |
|    - claw_event_router action: AGENT_EVENT_RUN_AGENT        |
|      Routes events through agent_flow_submit()              |
+-------------------------------------------------------------+
```

## Key Design Decisions

### 1. State Machine Instead of Ad-Hoc Branching

The original `claw_core` used implicit state through local variables and
nested loops. Agent Flow introduces an explicit `agent_state_machine_t`
with a transition table covering all valid state/event pairs.

Benefits:
- Observable: every state change is logged and hookable
- Testable: transitions can be unit-tested independently
- Extensible: custom transitions can be registered at runtime

### 2. Two-Phase Tool Execution

Inspired by Claude Code's `StreamingToolExecutor`, tools are partitioned
into two phases:

1. **Concurrent phase**: All `AGENT_TOOL_CONC_SAFE` tools spawn FreeRTOS
   tasks and run in parallel (bounded by `max_concurrent_tools`).
2. **Exclusive phase**: All `AGENT_TOOL_CONC_EXCLUSIVE` tools run serially
   under a single mutex.

This maximizes throughput for read-only tools (e.g., `cap_files_read`,
`cap_web_search`) while preventing races for mutating tools (e.g.,
`cap_files_write`, `cap_lua_run_script`).

### 3. Context Pipeline with Token Budget

Context assembly is no longer hard-coded in `claw_core.c`. Instead,
`agent_context_provider_t` functions are registered with a priority.
The pipeline:

1. Sorts providers by priority
2. Collects context from each provider
3. Accumulates token estimates
4. Skips providers that would exceed `token_budget`
5. Assembles final `system_prompt`, `messages_json`, `tools_json`

Providers can be added/removed dynamically, enabling runtime skill
loading and memory injection.

### 4. Streaming Event Propagation

The `agent_stream_callback_fn` allows the UI layer to receive:
- `TEXT_DELTA` — incremental text from LLM
- `TOOL_CALL_START/DELTA/DONE` — tool call assembly
- `DONE/ERROR` — completion events

This enables real-time UI updates without polling the response queue.

### 5. Cap Bridge for Backward Compatibility

`agent_flow_install_cap_bridge()` iterates over all registered
`claw_cap_descriptor_t` entries with `CLAW_CAP_FLAG_CALLABLE_BY_LLM` and
registers them as `agent_tool_descriptor_t` entries. This means:

- Existing capabilities work without modification
- New Agent Flow code can call old caps
- Old `claw_core` can coexist during migration

## State Transition Reference

```
IDLE --[START]--> CONTEXT_BUILD
CONTEXT_BUILD --[CONTEXT_READY]--> LLM_CALL
CONTEXT_BUILD --[COMPACT_REQUIRED]--> COMPACT
COMPACT --[CONTEXT_READY]--> LLM_CALL
LLM_CALL --[LLM_FIRST_CHUNK]--> STREAMING
STREAMING --[LLM_TOOL_CALL]--> TOOL_DISPATCH
STREAMING --[LLM_DONE]--> RESPONDING
TOOL_DISPATCH --[TOOL_BATCH_START]--> TOOL_EXECUTE
TOOL_EXECUTE --[TOOL_BATCH_DONE]--> OBSERVE
TOOL_EXECUTE --[ERROR]--> ERROR
OBSERVE --[CONTINUE]--> CONTEXT_BUILD
OBSERVE --[MAX_TURNS]--> DONE
RESPONDING --> (terminal)
DONE --> (terminal)
ERROR --[LLM_DONE]--> DONE
CANCELLED --> (terminal)

(All states --[CANCEL]--> CANCELLED)
```

## Integration Guide

### Minimal Integration in app_claw.c

```c
#include "claw_agent_flow.h"

static esp_err_t init_agent_flow(const app_claw_config_t *config)
{
    agent_flow_config_t flow_cfg = {
        .api_key = config->llm_api_key,
        .backend_type = config->llm_backend_type,
        .model = config->llm_model,
        .base_url = config->llm_base_url,
        .system_prompt = APP_SYSTEM_PROMPT,
        .supports_tools = true,
        .max_turns = 32,
        .token_budget = 8000,
        .task_stack_size = 16 * 1024,
        .task_priority = 5,
        .on_state_change = my_state_observer,
    };
    ESP_RETURN_ON_ERROR(agent_flow_init(&flow_cfg), TAG, "init failed");
    ESP_RETURN_ON_ERROR(agent_flow_install_cap_bridge(), TAG, "bridge failed");
    ESP_RETURN_ON_ERROR(agent_flow_start(), TAG, "start failed");
    return ESP_OK;
}
```

### Custom Context Provider

```c
static esp_err_t my_provider_collect(const agent_request_t *req,
                                      agent_context_item_t *out,
                                      void *ctx)
{
    (void)req; (void)ctx;
    out->kind = AGENT_CTX_KIND_SYSTEM_PROMPT;
    out->content = strdup("Custom system prompt injection");
    out->priority = 50;
    return ESP_OK;
}

static agent_context_provider_t my_provider = {
    .name = "my_custom",
    .kind = AGENT_CTX_KIND_SYSTEM_PROMPT,
    .collect = my_provider_collect,
    .priority = 50,
};

/* After agent_flow_init(): */
agent_flow_add_context_provider(&my_provider);
```

## Files

| File | Purpose |
|------|---------|
| `include/claw_agent_flow.h` | Main public API |
| `include/claw_agent_state_machine.h` | State machine API |
| `include/claw_agent_tool_orchestrator.h` | Tool registry + executor |
| `include/claw_agent_context_pipeline.h` | Context pipeline |
| `src/claw_agent_flow.c` | Orchestrator + Query Loop |
| `src/claw_agent_state_machine.c` | State machine implementation |
| `src/claw_agent_tool_orchestrator.c` | Tool registry + batch executor |
| `src/claw_agent_context_pipeline.c` | Pipeline + token estimation |
| `CMakeLists.txt` | ESP-IDF component build |
| `Kconfig` | Menuconfig options |
| `ARCHITECTURE.md` | This document |
