/*
 * SPDX-FileCopyrightText: 2026 Espressif Systems (Shanghai) CO LTD
 *
 * SPDX-License-Identifier: Apache-2.0
 */
#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "esp_err.h"
#include "freertos/FreeRTOS.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ========================================================================
 * Agent Flow Architecture — Core Definitions
 * ========================================================================
 * Inspired by Claude Code's query-loop + tool-orchestration design.
 *
 * Architecture Layers:
 *   ┌─────────────────────────────────────────┐
 *   │  Agent Orchestrator (claw_agent_flow)   │  ← lifecycle, config
 *   ├─────────────────────────────────────────┤
 *   │  Query Loop Engine                      │  ← LLM round-trip loop
 *   ├─────────────────────────────────────────┤
 *   │  Tool Orchestrator                      │  ← concurrency, deps, perms
 *   ├─────────────────────────────────────────┤
 *   │  State Machine                          │  ← state transitions
 *   ├─────────────────────────────────────────┤
 *   │  Context Pipeline                       │  ← provider chain
 *   └─────────────────────────────────────────┘
 * ======================================================================== */

/* ─── Agent State Machine ─── */

typedef enum {
    AGENT_STATE_IDLE = 0,
    AGENT_STATE_CONTEXT_BUILD,
    AGENT_STATE_LLM_CALL,
    AGENT_STATE_STREAMING,
    AGENT_STATE_TOOL_DISPATCH,
    AGENT_STATE_TOOL_EXECUTE,
    AGENT_STATE_OBSERVE,
    AGENT_STATE_RESPONDING,
    AGENT_STATE_COMPACT,
    AGENT_STATE_DONE,
    AGENT_STATE_ERROR,
    AGENT_STATE_CANCELLED,
} agent_state_t;

typedef enum {
    AGENT_EVENT_START = 0,
    AGENT_EVENT_CONTEXT_READY,
    AGENT_EVENT_LLM_FIRST_CHUNK,
    AGENT_EVENT_LLM_TOOL_CALL,
    AGENT_EVENT_LLM_TEXT_DELTA,
    AGENT_EVENT_LLM_DONE,
    AGENT_EVENT_TOOL_BATCH_START,
    AGENT_EVENT_TOOL_BATCH_DONE,
    AGENT_EVENT_TOOL_RESULT,
    AGENT_EVENT_CONTINUE,
    AGENT_EVENT_MAX_TURNS,
    AGENT_EVENT_COMPACT_REQUIRED,
    AGENT_EVENT_CANCEL,
    AGENT_EVENT_ERROR,
} agent_event_t;

/* ─── Tool Orchestration ─── */

typedef enum {
    AGENT_TOOL_PERM_ALLOW = 0,
    AGENT_TOOL_PERM_DENY,
    AGENT_TOOL_PERM_ASK,
} agent_tool_permission_t;

typedef enum {
    AGENT_TOOL_CONC_SAFE = 0,
    AGENT_TOOL_CONC_EXCLUSIVE,
} agent_tool_concurrency_t;

typedef enum {
    AGENT_TOOL_PHASE_PENDING = 0,
    AGENT_TOOL_PHASE_DISPATCHED,
    AGENT_TOOL_PHASE_EXECUTING,
    AGENT_TOOL_PHASE_DONE,
    AGENT_TOOL_PHASE_FAILED,
    AGENT_TOOL_PHASE_DENIED,
} agent_tool_phase_t;

typedef struct {
    char id[48];
    char name[64];
    char arguments_json[1024];
    agent_tool_concurrency_t concurrency;
    agent_tool_permission_t permission;
    agent_tool_phase_t phase;
    char result_json[2048];
    uint32_t duration_ms;
    esp_err_t error;
} agent_tool_call_t;

typedef struct {
    agent_tool_call_t *items;
    size_t count;
    size_t capacity;
    uint32_t max_concurrent;
} agent_tool_batch_t;

/* ─── Context Pipeline ─── */

typedef enum {
    AGENT_CTX_KIND_SYSTEM_PROMPT = 0,
    AGENT_CTX_KIND_MEMORY,
    AGENT_CTX_KIND_SKILL,
    AGENT_CTX_KIND_TOOL_DEF,
    AGENT_CTX_KIND_MESSAGE_HISTORY,
    AGENT_CTX_KIND_CURRENT_TURN,
} agent_ctx_kind_t;

typedef struct {
    agent_ctx_kind_t kind;
    char *content;
    size_t content_len;
    uint32_t priority;
    char provider_name[32];
} agent_context_item_t;

typedef struct {
    agent_context_item_t *items;
    size_t count;
    size_t capacity;
    uint32_t total_tokens;
    uint32_t token_budget;
} agent_context_pipeline_t;

typedef esp_err_t (*agent_context_provider_fn)(const struct agent_request *req,
                                                agent_context_item_t *out_item,
                                                void *user_ctx);

typedef struct {
    char name[32];
    agent_ctx_kind_t kind;
    agent_context_provider_fn collect;
    void *user_ctx;
    uint32_t priority;
} agent_context_provider_t;

/* ─── Request / Response ─── */

typedef enum {
    AGENT_REQ_FLAG_PUBLISH_OUT_MSG   = 1U << 0,
    AGENT_REQ_FLAG_SKIP_HISTORY      = 1U << 1,
    AGENT_REQ_FLAG_FORCE_COMPACT     = 1U << 2,
    AGENT_REQ_FLAG_STREAM_RESPONSE   = 1U << 3,
} agent_request_flags_t;

typedef struct agent_request {
    uint32_t request_id;
    uint32_t flags;
    const char *session_id;
    const char *user_text;
    const char *source_channel;
    const char *source_chat_id;
    const char *source_sender_id;
    const char *source_message_id;
    const char *source_cap;
    const char *target_channel;
    const char *target_chat_id;
    uint32_t max_turns;
} agent_request_t;

typedef enum {
    AGENT_RESP_STATUS_OK = 0,
    AGENT_RESP_STATUS_ERROR,
    AGENT_RESP_STATUS_CANCELLED,
    AGENT_RESP_STATUS_MAX_TURNS,
    AGENT_RESP_STATUS_PROMPT_TOO_LONG,
} agent_response_status_t;

typedef struct {
    uint32_t request_id;
    agent_response_status_t status;
    char *text;
    char *error_message;
    uint32_t turns_consumed;
    uint32_t tools_invoked;
    uint32_t tokens_consumed;
    char *tool_calls_csv;
    char *context_providers_csv;
} agent_response_t;

/* ─── Streaming Events ─── */

typedef enum {
    AGENT_STREAM_EVENT_TEXT_DELTA = 0,
    AGENT_STREAM_EVENT_TOOL_CALL_START,
    AGENT_STREAM_EVENT_TOOL_CALL_DELTA,
    AGENT_STREAM_EVENT_TOOL_CALL_DONE,
    AGENT_STREAM_EVENT_DONE,
    AGENT_STREAM_EVENT_ERROR,
} agent_stream_event_type_t;

typedef struct {
    agent_stream_event_type_t type;
    uint32_t request_id;
    union {
        struct { const char *delta; size_t len; } text;
        struct { const char *tool_name; const char *tool_id; } tool_start;
        struct { const char *tool_id; const char *args_delta; } tool_delta;
        struct { const char *tool_id; const char *args_json; } tool_done;
        struct { const char *error_msg; } error;
    };
} agent_stream_event_t;

typedef void (*agent_stream_callback_fn)(const agent_stream_event_t *event,
                                         void *user_ctx);

/* ─── Lifecycle Hooks ─── */

typedef esp_err_t (*agent_hook_pre_query_fn)(const agent_request_t *req,
                                              void *user_ctx);
typedef esp_err_t (*agent_hook_post_query_fn)(const agent_response_t *resp,
                                               void *user_ctx);
typedef esp_err_t (*agent_hook_pre_tool_fn)(agent_tool_call_t *tool,
                                             void *user_ctx);
typedef esp_err_t (*agent_hook_post_tool_fn)(agent_tool_call_t *tool,
                                              void *user_ctx);
typedef esp_err_t (*agent_hook_permission_fn)(const char *tool_name,
                                               const char *arguments_json,
                                               agent_tool_permission_t *out_perm,
                                               void *user_ctx);
typedef esp_err_t (*agent_hook_state_fn)(agent_state_t old_state,
                                          agent_state_t new_state,
                                          agent_event_t event,
                                          void *user_ctx);

/* ─── Agent Config ─── */

typedef struct {
    /* LLM config */
    const char *api_key;
    const char *backend_type;
    const char *model;
    const char *base_url;
    const char *auth_type;
    const char *max_tokens_field;
    uint32_t timeout_ms;
    uint32_t max_tokens;
    size_t image_max_bytes;
    bool supports_tools;
    bool supports_vision;
    bool image_remote_url_only;
    const char *system_prompt;

    /* Query loop */
    uint32_t max_tool_iterations;
    uint32_t max_turns;
    uint32_t token_budget;
    uint32_t compact_threshold;

    /* Tool orchestration */
    uint32_t max_concurrent_tools;
    bool enable_permission_checks;

    /* Streaming */
    agent_stream_callback_fn stream_cb;
    void *stream_user_ctx;

    /* Hooks */
    agent_hook_pre_query_fn on_pre_query;
    void *on_pre_query_ctx;
    agent_hook_post_query_fn on_post_query;
    void *on_post_query_ctx;
    agent_hook_pre_tool_fn on_pre_tool;
    void *on_pre_tool_ctx;
    agent_hook_post_tool_fn on_post_tool;
    void *on_post_tool_ctx;
    agent_hook_permission_fn on_permission;
    void *on_permission_ctx;
    agent_hook_state_fn on_state_change;
    void *on_state_change_ctx;

    /* Task config */
    uint32_t task_stack_size;
    UBaseType_t task_priority;
    BaseType_t task_core;
    uint32_t request_queue_len;
    uint32_t response_queue_len;
} agent_flow_config_t;

/* ─── Public API ─── */

esp_err_t agent_flow_init(const agent_flow_config_t *config);
esp_err_t agent_flow_start(void);
esp_err_t agent_flow_stop(void);

esp_err_t agent_flow_submit(const agent_request_t *request, uint32_t timeout_ms);
esp_err_t agent_flow_receive(agent_response_t *response, uint32_t timeout_ms);
esp_err_t agent_flow_receive_for(uint32_t request_id,
                                  agent_response_t *response,
                                  uint32_t timeout_ms);
void agent_flow_response_free(agent_response_t *response);

esp_err_t agent_flow_cancel(uint32_t request_id);
esp_err_t agent_flow_cancel_all(void);

esp_err_t agent_flow_add_context_provider(const agent_context_provider_t *provider);
esp_err_t agent_flow_remove_context_provider(const char *name);

agent_state_t agent_flow_get_state(void);
esp_err_t agent_flow_get_last_result(agent_response_t *out_response);

/* ─── Tool Registry (for orchestrator) ─── */

typedef esp_err_t (*agent_tool_execute_fn)(const char *input_json,
                                            char *output,
                                            size_t output_size,
                                            void *user_ctx);

typedef struct {
    char name[64];
    char description[256];
    char input_schema_json[1024];
    agent_tool_concurrency_t concurrency;
    agent_tool_execute_fn execute;
    void *user_ctx;
} agent_tool_descriptor_t;

esp_err_t agent_flow_register_tool(const agent_tool_descriptor_t *tool);
esp_err_t agent_flow_unregister_tool(const char *name);

/* ─── Integration with existing claw_core ─── */

/* Adapter: bridge agent_flow requests to existing claw_cap system */
esp_err_t agent_flow_install_cap_bridge(void);

#ifdef __cplusplus
}
#endif
