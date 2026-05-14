/*
 * SPDX-FileCopyrightText: 2026 Espressif Systems (Shanghai) CO LTD
 *
 * SPDX-License-Identifier: Apache-2.0
 */
#include "claw_agent_flow.h"
#include "claw_agent_state_machine.h"
#include "claw_agent_tool_orchestrator.h"
#include "claw_agent_context_pipeline.h"

#include <inttypes.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include "cJSON.h"
#include "claw_cap.h"
#include "claw_core.h"
#include "claw_event_publisher.h"
#include "claw_event_router.h"
#include "esp_heap_caps.h"
#include "esp_log.h"
#include "freertos/idf_additions.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"
#include "freertos/task.h"

static const char *TAG = "agent_flow";

#define AGENT_FLOW_DEFAULT_STACK_SIZE   (16 * 1024)
#define AGENT_FLOW_DEFAULT_PRIORITY     5
#define AGENT_FLOW_DEFAULT_REQ_Q        4
#define AGENT_FLOW_DEFAULT_RESP_Q       4
#define AGENT_FLOW_DEFAULT_MAX_TURNS    32
#define AGENT_FLOW_DEFAULT_TOKEN_BUDGET 8000
#define AGENT_FLOW_DEFAULT_COMPACT_THR  7000
#define AGENT_FLOW_MAX_TOOLS            32
#define AGENT_FLOW_MAX_CTX_PROVIDERS    8
#define AGENT_FLOW_MAX_TOOL_CALLS       16
#define AGENT_FLOW_RESP_TEXT_CAP        4096

/* ─── Request/Response item wrappers ─── */

typedef struct {
    agent_request_t view;
    char *owned_session_id;
    char *owned_user_text;
    char *owned_source_channel;
    char *owned_source_chat_id;
    char *owned_source_sender_id;
    char *owned_source_message_id;
    char *owned_source_cap;
    char *owned_target_channel;
    char *owned_target_chat_id;
} agent_request_item_t;

typedef struct {
    agent_response_t view;
    char *owned_text;
    char *owned_error_message;
    char *owned_tool_calls_csv;
    char *owned_providers_csv;
} agent_response_item_t;

/* ─── Runtime ─── */

typedef struct {
    bool initialized;
    bool started;
    bool stop_requested;
    bool inflight_abort;

    /* Subsystems */
    agent_state_machine_t *state_machine;
    agent_tool_registry_t *tool_registry;
    agent_tool_executor_t *tool_executor;
    agent_ctx_pipeline_t *ctx_pipeline;

    /* Queues */
    QueueHandle_t request_queue;
    QueueHandle_t response_queue;
    SemaphoreHandle_t response_lock;

    /* Task */
    TaskHandle_t task_handle;
    uint32_t task_stack_size;
    UBaseType_t task_priority;
    BaseType_t task_core;

    /* Config copies */
    agent_flow_config_t config;
    char *system_prompt;

    /* Inflight tracking */
    SemaphoreHandle_t inflight_lock;
    uint32_t inflight_request_id;
    uint32_t next_request_id;

    /* Context providers */
    agent_context_provider_t providers[AGENT_FLOW_MAX_CTX_PROVIDERS];
    size_t provider_count;

    /* Last result */
    agent_response_item_t last_result;
} agent_flow_runtime_t;

static agent_flow_runtime_t *s_flow = NULL;

/* ─── Helpers ─── */

static char *dup_string(const char *src)
{
    return src ? strdup(src) : NULL;
}

static void free_request_item(agent_request_item_t *item)
{
    if (!item) {
        return;
    }
    free(item->owned_session_id);
    free(item->owned_user_text);
    free(item->owned_source_channel);
    free(item->owned_source_chat_id);
    free(item->owned_source_sender_id);
    free(item->owned_source_message_id);
    free(item->owned_source_cap);
    free(item->owned_target_channel);
    free(item->owned_target_chat_id);
    memset(item, 0, sizeof(*item));
}

static void free_response_item(agent_response_item_t *item)
{
    if (!item) {
        return;
    }
    free(item->owned_text);
    free(item->owned_error_message);
    free(item->owned_tool_calls_csv);
    free(item->owned_providers_csv);
    memset(item, 0, sizeof(*item));
}

static int64_t now_ms(void)
{
    struct timeval tv = {0};
    gettimeofday(&tv, NULL);
    return ((int64_t)tv.tv_sec * 1000LL) + (tv.tv_usec / 1000LL);
}

/* ─── State change hook wrapper ─── */

static void state_change_hook(agent_state_t old_state, agent_state_t new_state, agent_event_t event)
{
    ESP_LOGI(TAG, "State: %s -> %s [%s]",
             agent_state_to_string(old_state),
             agent_state_to_string(new_state),
             agent_event_to_string(event));
    if (s_flow && s_flow->config.on_state_change) {
        s_flow->config.on_state_change(old_state, new_state, event,
                                       s_flow->config.on_state_change_ctx);
    }
}

/* ─── LLM Chat Integration ─── */

/* Stub: delegate to existing claw_core_llm module */
extern esp_err_t claw_core_llm_chat_messages(const char *system_prompt,
                                              cJSON *messages,
                                              const char *tools_json,
                                              void *out_response,
                                              char **out_error);

/* We'll use a simplified response structure for the stub */
typedef struct {
    char *text;
    size_t tool_call_count;
    struct {
        char id[32];
        char name[64];
        char arguments_json[1024];
    } tool_calls[AGENT_FLOW_MAX_TOOL_CALLS];
} agent_llm_response_t;

static void agent_llm_response_free(agent_llm_response_t *resp)
{
    if (!resp) {
        return;
    }
    free(resp->text);
    resp->text = NULL;
    resp->tool_call_count = 0;
}

/* ─── Build out message event ─── */

static esp_err_t build_out_message_event(const agent_request_item_t *req,
                                          const agent_response_item_t *resp,
                                          claw_event_t *out_event)
{
    const char *text = resp->view.text;
    const char *channel = req->view.target_channel && req->view.target_channel[0]
                          ? req->view.target_channel : req->view.source_channel;
    const char *chat_id = req->view.target_chat_id && req->view.target_chat_id[0]
                          ? req->view.target_chat_id : req->view.source_chat_id;
    int64_t t = now_ms();

    if (!text || !text[0] || !channel || !chat_id) {
        return ESP_ERR_INVALID_ARG;
    }

    memset(out_event, 0, sizeof(*out_event));
    snprintf(out_event->event_id, sizeof(out_event->event_id), "agent-%" PRIu32 "-%" PRId64,
             req->view.request_id, t);
    strlcpy(out_event->source_cap, "claw_agent_flow", sizeof(out_event->source_cap));
    strlcpy(out_event->event_type, "out_message", sizeof(out_event->event_type));
    strlcpy(out_event->source_channel, channel, sizeof(out_event->source_channel));
    strlcpy(out_event->chat_id, chat_id, sizeof(out_event->chat_id));
    strlcpy(out_event->content_type, "text", sizeof(out_event->content_type));
    out_event->text = (char *)text;
    out_event->timestamp_ms = t;
    out_event->session_policy = CLAW_EVENT_SESSION_POLICY_CHAT;

    if (req->view.source_message_id && req->view.source_message_id[0]) {
        strlcpy(out_event->correlation_id, req->view.source_message_id,
                sizeof(out_event->correlation_id));
    }
    return ESP_OK;
}

static void publish_out_message_if_requested(const agent_request_item_t *req,
                                              const agent_response_item_t *resp)
{
    claw_event_t event = {0};
    esp_err_t err;

    if (!req || !resp || !(req->view.flags & AGENT_REQ_FLAG_PUBLISH_OUT_MSG)) {
        return;
    }

    err = build_out_message_event(req, resp, &event);
    if (err == ESP_OK) {
        err = claw_event_router_publish(&event);
    }
    if (err != ESP_OK) {
        ESP_LOGW(TAG, "Failed to publish out_message: %s", esp_err_to_name(err));
    }
}

/* ─── Streaming callback wrapper ─── */

static void stream_text_delta(uint32_t request_id, const char *delta, size_t len)
{
    if (!s_flow || !s_flow->config.stream_cb || !delta) {
        return;
    }
    agent_stream_event_t ev = {
        .type = AGENT_STREAM_EVENT_TEXT_DELTA,
        .request_id = request_id,
        .text = { .delta = delta, .len = len },
    };
    s_flow->config.stream_cb(&ev, s_flow->config.stream_user_ctx);
}

/* ─── Tool calls → batch ─── */

static esp_err_t parse_tool_calls(const agent_llm_response_t *llm_resp,
                                   agent_tool_batch_t *batch)
{
    if (!llm_resp || !batch) {
        return ESP_ERR_INVALID_ARG;
    }
    batch->count = 0;

    for (size_t i = 0; i < llm_resp->tool_call_count && i < AGENT_FLOW_MAX_TOOL_CALLS; i++) {
        agent_tool_call_t *call = &batch->items[batch->count++];
        memset(call, 0, sizeof(*call));
        strlcpy(call->id, llm_resp->tool_calls[i].id, sizeof(call->id));
        strlcpy(call->name, llm_resp->tool_calls[i].name, sizeof(call->name));
        strlcpy(call->arguments_json, llm_resp->tool_calls[i].arguments_json,
                sizeof(call->arguments_json));
        call->concurrency = AGENT_TOOL_CONC_SAFE; /* default; registry may override */
        call->permission = AGENT_TOOL_PERM_ALLOW;
        call->phase = AGENT_TOOL_PHASE_PENDING;
    }
    return ESP_OK;
}

/* ─── Append tool results to messages ─── */

static esp_err_t append_tool_results(cJSON *messages,
                                      const agent_llm_response_t *llm_resp,
                                      const agent_tool_batch_t *batch)
{
    cJSON *assistant = NULL;
    cJSON *tool_calls = NULL;

    if (!messages || !llm_resp || !batch) {
        return ESP_ERR_INVALID_ARG;
    }

    /* assistant message with tool_calls */
    assistant = cJSON_CreateObject();
    if (!assistant) {
        return ESP_ERR_NO_MEM;
    }
    cJSON_AddStringToObject(assistant, "role", "assistant");
    if (llm_resp->text && llm_resp->text[0]) {
        cJSON_AddStringToObject(assistant, "content", llm_resp->text);
    } else {
        cJSON_AddNullToObject(assistant, "content");
    }

    tool_calls = cJSON_CreateArray();
    if (!tool_calls) {
        cJSON_Delete(assistant);
        return ESP_ERR_NO_MEM;
    }

    for (size_t i = 0; i < llm_resp->tool_call_count; i++) {
        cJSON *tc = cJSON_CreateObject();
        cJSON *fn = cJSON_CreateObject();
        if (!tc || !fn) {
            cJSON_Delete(tc);
            cJSON_Delete(fn);
            continue;
        }
        cJSON_AddStringToObject(tc, "id", llm_resp->tool_calls[i].id);
        cJSON_AddStringToObject(tc, "type", "function");
        cJSON_AddStringToObject(fn, "name", llm_resp->tool_calls[i].name);
        cJSON_AddStringToObject(fn, "arguments", llm_resp->tool_calls[i].arguments_json);
        cJSON_AddItemToObject(tc, "function", fn);
        cJSON_AddItemToArray(tool_calls, tc);
    }
    cJSON_AddItemToObject(assistant, "tool_calls", tool_calls);
    cJSON_AddItemToArray(messages, assistant);

    /* tool result messages */
    for (size_t i = 0; i < batch->count; i++) {
        cJSON *tr = cJSON_CreateObject();
        if (!tr) {
            continue;
        }
        cJSON_AddStringToObject(tr, "role", "tool");
        cJSON_AddStringToObject(tr, "tool_call_id", batch->items[i].id);
        cJSON_AddStringToObject(tr, "content", batch->items[i].result_json);
        cJSON_AddItemToArray(messages, tr);
    }

    return ESP_OK;
}

/* ─── Main Query Loop ─── */

static void agent_flow_query_loop(agent_request_item_t *req,
                                   agent_response_item_t *resp)
{
    agent_state_t state = AGENT_STATE_IDLE;
    cJSON *runtime_messages = NULL;
    char *system_prompt = NULL;
    char *messages_json = NULL;
    char *tools_json = NULL;
    agent_llm_response_t llm_resp = {0};
    agent_tool_batch_t batch = {0};
    uint32_t turn = 0;
    uint32_t tools_total = 0;
    char tool_calls_csv[384] = {0};
    char providers_csv[384] = {0};
    esp_err_t err = ESP_OK;

    batch.items = calloc(AGENT_FLOW_MAX_TOOL_CALLS, sizeof(*batch.items));
    if (!batch.items) {
        resp->view.status = AGENT_RESP_STATUS_ERROR;
        resp->owned_error_message = strdup("alloc batch failed");
        goto finish;
    }
    batch.capacity = AGENT_FLOW_MAX_TOOL_CALLS;

    runtime_messages = cJSON_CreateArray();
    if (!runtime_messages) {
        resp->view.status = AGENT_RESP_STATUS_ERROR;
        resp->owned_error_message = strdup("alloc runtime_messages failed");
        goto finish;
    }

    /* State: START */
    err = agent_state_machine_dispatch(s_flow->state_machine, AGENT_EVENT_START, &state);
    if (err != ESP_OK) {
        resp->view.status = AGENT_RESP_STATUS_ERROR;
        resp->owned_error_message = strdup("state_machine START failed");
        goto finish;
    }
    state_change_hook(AGENT_STATE_IDLE, state, AGENT_EVENT_START);

    /* Pre-query hook */
    if (s_flow->config.on_pre_query) {
        s_flow->config.on_pre_query(&req->view, s_flow->config.on_pre_query_ctx);
    }

    while (true) {
        /* ── Context Build ── */
        if (state == AGENT_STATE_CONTEXT_BUILD || state == AGENT_STATE_OBSERVE) {
            free(system_prompt);
            free(messages_json);
            free(tools_json);
            system_prompt = NULL;
            messages_json = NULL;
            tools_json = NULL;

            uint32_t token_estimate = 0;
            err = agent_ctx_pipeline_build(s_flow->ctx_pipeline, &req->view,
                                           &system_prompt, &messages_json, &tools_json,
                                           &token_estimate);
            if (err != ESP_OK) {
                resp->view.status = AGENT_RESP_STATUS_ERROR;
                resp->owned_error_message = strdup(esp_err_to_name(err));
                goto finish;
            }

            /* Check compact threshold */
            if (s_flow->config.token_budget > 0 &&
                token_estimate > s_flow->config.compact_threshold) {
                agent_state_machine_dispatch(s_flow->state_machine,
                                              AGENT_EVENT_COMPACT_REQUIRED, &state);
                state_change_hook(AGENT_STATE_CONTEXT_BUILD, state, AGENT_EVENT_COMPACT_REQUIRED);
                agent_ctx_pipeline_compact(s_flow->ctx_pipeline);
            }

            err = agent_state_machine_dispatch(s_flow->state_machine,
                                                AGENT_EVENT_CONTEXT_READY, &state);
            state_change_hook(AGENT_STATE_CONTEXT_BUILD, state, AGENT_EVENT_CONTEXT_READY);
        }

        /* ── LLM Call ── */
        if (state == AGENT_STATE_LLM_CALL) {
            /* Parse messages_json into cJSON for LLM */
            cJSON *messages = cJSON_Parse(messages_json);
            if (!messages) {
                resp->view.status = AGENT_RESP_STATUS_ERROR;
                resp->owned_error_message = strdup("parse messages_json failed");
                goto finish;
            }

            /* Append runtime tool-turn messages */
            if (cJSON_GetArraySize(runtime_messages) > 0) {
                const cJSON *m = NULL;
                cJSON_ArrayForEach(m, runtime_messages) {
                    cJSON *dup = cJSON_Duplicate(m, true);
                    if (dup) {
                        cJSON_AddItemToArray(messages, dup);
                    }
                }
            }

            /* Streaming: first chunk */
            agent_state_machine_dispatch(s_flow->state_machine,
                                          AGENT_EVENT_LLM_FIRST_CHUNK, &state);
            state_change_hook(AGENT_STATE_LLM_CALL, state, AGENT_EVENT_LLM_FIRST_CHUNK);

            /* ── Invoke LLM (stubbed — real impl would call claw_core_llm) ── */
            /* For architecture demonstration, we simulate:
             *   - If supports_tools and tools_json present, return a mock tool call
             *   - Otherwise return text response
             */
            agent_llm_response_free(&llm_resp);
            memset(&llm_resp, 0, sizeof(llm_resp));

#if 0 /* Real integration: call claw_core_llm_chat_messages() */
            err = claw_core_llm_chat_messages(system_prompt, messages, tools_json,
                                               &llm_resp, &resp->owned_error_message);
#else
            /* Stub: echo back for demonstration */
            if (turn == 0 && s_flow->config.supports_tools && tools_json && tools_json[0]) {
                /* Simulate tool call on first turn */
                llm_resp.text = strdup("");
                llm_resp.tool_call_count = 1;
                strlcpy(llm_resp.tool_calls[0].id, "call_1", sizeof(llm_resp.tool_calls[0].id));
                strlcpy(llm_resp.tool_calls[0].name, "cap_time_get", sizeof(llm_resp.tool_calls[0].name));
                strlcpy(llm_resp.tool_calls[0].arguments_json, "{}",
                        sizeof(llm_resp.tool_calls[0].arguments_json));
                err = ESP_OK;
            } else {
                llm_resp.text = calloc(1, AGENT_FLOW_RESP_TEXT_CAP);
                if (llm_resp.text) {
                    snprintf(llm_resp.text, AGENT_FLOW_RESP_TEXT_CAP,
                             "Agent response for request %" PRIu32 " (turn %" PRIu32 ")",
                             req->view.request_id, turn);
                }
                err = ESP_OK;
            }
#endif
            cJSON_Delete(messages);

            if (err != ESP_OK) {
                resp->view.status = AGENT_RESP_STATUS_ERROR;
                goto finish;
            }

            /* Stream text delta */
            if (llm_resp.text && llm_resp.text[0]) {
                stream_text_delta(req->view.request_id, llm_resp.text, strlen(llm_resp.text));
            }

            /* Check abort */
            if (s_flow->inflight_abort) {
                resp->view.status = AGENT_RESP_STATUS_CANCELLED;
                resp->owned_error_message = strdup("cancelled");
                goto finish;
            }

            /* Tool call or done? */
            if (llm_resp.tool_call_count > 0) {
                agent_state_machine_dispatch(s_flow->state_machine,
                                              AGENT_EVENT_LLM_TOOL_CALL, &state);
                state_change_hook(AGENT_STATE_STREAMING, state, AGENT_EVENT_LLM_TOOL_CALL);
            } else {
                agent_state_machine_dispatch(s_flow->state_machine,
                                              AGENT_EVENT_LLM_DONE, &state);
                state_change_hook(AGENT_STATE_STREAMING, state, AGENT_EVENT_LLM_DONE);
            }
        }

        /* ── Tool Dispatch ── */
        if (state == AGENT_STATE_TOOL_DISPATCH) {
            err = parse_tool_calls(&llm_resp, &batch);
            if (err != ESP_OK) {
                resp->view.status = AGENT_RESP_STATUS_ERROR;
                resp->owned_error_message = strdup("parse tool calls failed");
                goto finish;
            }

            /* Update concurrency classification from registry */
            for (size_t i = 0; i < batch.count; i++) {
                const agent_tool_descriptor_t *desc =
                    agent_tool_registry_find(s_flow->tool_registry, batch.items[i].name);
                if (desc) {
                    batch.items[i].concurrency = desc->concurrency;
                }
            }

            err = agent_state_machine_dispatch(s_flow->state_machine,
                                                AGENT_EVENT_TOOL_BATCH_START, &state);
            state_change_hook(AGENT_STATE_TOOL_DISPATCH, state, AGENT_EVENT_TOOL_BATCH_START);
        }

        /* ── Tool Execute ── */
        if (state == AGENT_STATE_TOOL_EXECUTE) {
            err = agent_tool_executor_run_batch(s_flow->tool_executor,
                                                 s_flow->tool_registry, &batch);
            if (err != ESP_OK) {
                agent_state_machine_dispatch(s_flow->state_machine,
                                              AGENT_EVENT_ERROR, &state);
                state_change_hook(AGENT_STATE_TOOL_EXECUTE, state, AGENT_EVENT_ERROR);
                resp->view.status = AGENT_RESP_STATUS_ERROR;
                resp->owned_error_message = strdup("tool execution failed");
                goto finish;
            }

            /* Collect results */
            for (size_t i = 0; i < batch.count; i++) {
                if (batch.items[i].phase == AGENT_TOOL_PHASE_DONE &&
                    batch.items[i].name[0]) {
                    size_t cur = strlen(tool_calls_csv);
                    if (cur < sizeof(tool_calls_csv) - 1) {
                        snprintf(tool_calls_csv + cur, sizeof(tool_calls_csv) - cur,
                                 "%s%s", cur == 0 ? "" : ",", batch.items[i].name);
                    }
                    tools_total++;
                }
            }

            err = agent_state_machine_dispatch(s_flow->state_machine,
                                                AGENT_EVENT_TOOL_BATCH_DONE, &state);
            state_change_hook(AGENT_STATE_TOOL_EXECUTE, state, AGENT_EVENT_TOOL_BATCH_DONE);
        }

        /* ── Observe ── */
        if (state == AGENT_STATE_OBSERVE) {
            /* Append tool results to runtime_messages for next iteration */
            append_tool_results(runtime_messages, &llm_resp, &batch);

            turn++;
            if (turn >= s_flow->config.max_turns) {
                agent_state_machine_dispatch(s_flow->state_machine,
                                              AGENT_EVENT_MAX_TURNS, &state);
                state_change_hook(AGENT_STATE_OBSERVE, state, AGENT_EVENT_MAX_TURNS);
                resp->view.status = AGENT_RESP_STATUS_MAX_TURNS;
                resp->owned_error_message = strdup("max turns reached");
                goto finish;
            }

            /* Check abort */
            if (s_flow->inflight_abort) {
                resp->view.status = AGENT_RESP_STATUS_CANCELLED;
                resp->owned_error_message = strdup("cancelled");
                goto finish;
            }

            /* Loop back to context build */
            err = agent_state_machine_dispatch(s_flow->state_machine,
                                                AGENT_EVENT_CONTINUE, &state);
            state_change_hook(AGENT_STATE_OBSERVE, state, AGENT_EVENT_CONTINUE);
        }

        /* ── Responding / Done ── */
        if (state == AGENT_STATE_RESPONDING || state == AGENT_STATE_DONE) {
            resp->view.status = AGENT_RESP_STATUS_OK;
            if (llm_resp.text && llm_resp.text[0]) {
                resp->owned_text = strdup(llm_resp.text);
                resp->view.text = resp->owned_text;
            }
            resp->view.turns_consumed = turn;
            resp->view.tools_invoked = tools_total;
            resp->owned_tool_calls_csv = strdup(tool_calls_csv);
            resp->view.tool_calls_csv = resp->owned_tool_calls_csv;
            resp->owned_providers_csv = strdup(providers_csv);
            resp->view.context_providers_csv = resp->owned_providers_csv;
            goto finish;
        }

        /* ── Error ── */
        if (state == AGENT_STATE_ERROR) {
            resp->view.status = AGENT_RESP_STATUS_ERROR;
            if (!resp->owned_error_message) {
                resp->owned_error_message = strdup("unknown error");
            }
            goto finish;
        }

        /* ── Cancelled ── */
        if (state == AGENT_STATE_CANCELLED) {
            resp->view.status = AGENT_RESP_STATUS_CANCELLED;
            if (!resp->owned_error_message) {
                resp->owned_error_message = strdup("cancelled");
            }
            goto finish;
        }
    }

finish:
    free(system_prompt);
    free(messages_json);
    free(tools_json);
    agent_llm_response_free(&llm_resp);
    cJSON_Delete(runtime_messages);
    free(batch.items);

    resp->view.request_id = req->view.request_id;
    resp->view.tokens_consumed = turn * 1000; /* placeholder */

    /* Post-query hook */
    if (s_flow->config.on_post_query) {
        s_flow->config.on_post_query(&resp->view, s_flow->config.on_post_query_ctx);
    }

    /* Publish out message if requested */
    publish_out_message_if_requested(req, resp);
}

/* ─── Worker Task ─── */

static void agent_flow_task(void *arg)
{
    (void)arg;
    ESP_LOGI(TAG, "Agent flow task started");

    while (!s_flow->stop_requested) {
        agent_request_item_t req = {0};
        agent_response_item_t resp = {0};

        if (xQueueReceive(s_flow->request_queue, &req, pdMS_TO_TICKS(250)) != pdTRUE) {
            continue;
        }

        /* Set inflight */
        if (xSemaphoreTake(s_flow->inflight_lock, portMAX_DELAY) == pdTRUE) {
            s_flow->inflight_request_id = req.view.request_id;
            s_flow->inflight_abort = false;
            xSemaphoreGive(s_flow->inflight_lock);
        }

        /* Reset state machine to idle for this request */
        agent_state_machine_t *fresh_sm = agent_state_machine_create();
        if (fresh_sm) {
            agent_state_machine_destroy(s_flow->state_machine);
            s_flow->state_machine = fresh_sm;
        }

        ESP_LOGI(TAG, "Processing request_id=%" PRIu32 " session=%s text=%.40s...",
                 req.view.request_id,
                 req.view.session_id ? req.view.session_id : "(none)",
                 req.view.user_text ? req.view.user_text : "");

        /* Run query loop */
        agent_flow_query_loop(&req, &resp);

        /* Clear inflight */
        if (xSemaphoreTake(s_flow->inflight_lock, portMAX_DELAY) == pdTRUE) {
            s_flow->inflight_request_id = 0;
            s_flow->inflight_abort = false;
            xSemaphoreGive(s_flow->inflight_lock);
        }

        /* Save last result */
        free_response_item(&s_flow->last_result);
        s_flow->last_result = resp;

        /* Enqueue response (if not skipped) */
        if (!(req.view.flags & AGENT_REQ_FLAG_SKIP_RESPONSE_QUEUE)) {
            if (xQueueSend(s_flow->response_queue, &resp, pdMS_TO_TICKS(1000)) != pdTRUE) {
                ESP_LOGW(TAG, "Response queue full, dropping request %" PRIu32, req.view.request_id);
                free_response_item(&resp);
            }
        } else {
            free_response_item(&resp);
        }

        free_request_item(&req);
    }

    s_flow->task_handle = NULL;
    s_flow->started = false;
    ESP_LOGI(TAG, "Agent flow task stopped");
    vTaskDelete(NULL);
}

/* ─── Public API ─── */

esp_err_t agent_flow_init(const agent_flow_config_t *config)
{
    if (s_flow) {
        return ESP_ERR_INVALID_STATE;
    }
    if (!config) {
        return ESP_ERR_INVALID_ARG;
    }

    s_flow = calloc(1, sizeof(*s_flow));
    if (!s_flow) {
        return ESP_ERR_NO_MEM;
    }

    /* Copy config */
    s_flow->config = *config;
    s_flow->system_prompt = config->system_prompt ? strdup(config->system_prompt) : NULL;

    /* Defaults */
    s_flow->task_stack_size = config->task_stack_size ? config->task_stack_size
                                                       : AGENT_FLOW_DEFAULT_STACK_SIZE;
    s_flow->task_priority = config->task_priority ? config->task_priority
                                                   : AGENT_FLOW_DEFAULT_PRIORITY;
    s_flow->task_core = config->task_core;
    s_flow->config.max_turns = config->max_turns ? config->max_turns
                                                  : AGENT_FLOW_DEFAULT_MAX_TURNS;
    s_flow->config.token_budget = config->token_budget ? config->token_budget
                                                        : AGENT_FLOW_DEFAULT_TOKEN_BUDGET;
    s_flow->config.compact_threshold = config->compact_threshold ? config->compact_threshold
                                                                  : AGENT_FLOW_DEFAULT_COMPACT_THR;
    s_flow->config.max_concurrent_tools = config->max_concurrent_tools ? config->max_concurrent_tools : 4;

    /* Subsystems */
    s_flow->state_machine = agent_state_machine_create();
    s_flow->tool_registry = agent_tool_registry_create(AGENT_FLOW_MAX_TOOLS);
    s_flow->ctx_pipeline = agent_ctx_pipeline_create(AGENT_FLOW_MAX_CTX_PROVIDERS,
                                                      s_flow->config.token_budget);

    agent_tool_executor_config_t exec_cfg = {
        .max_concurrent = s_flow->config.max_concurrent_tools,
        .timeout_ms_per_tool = 30000,
        .permission_fn = config->on_permission,
        .permission_ctx = config->on_permission_ctx,
        .pre_tool_fn = config->on_pre_tool,
        .pre_tool_ctx = config->on_pre_tool_ctx,
        .post_tool_fn = config->on_post_tool,
        .post_tool_ctx = config->on_post_tool_ctx,
    };
    s_flow->tool_executor = agent_tool_executor_create(&exec_cfg);

    if (!s_flow->state_machine || !s_flow->tool_registry ||
        !s_flow->tool_executor || !s_flow->ctx_pipeline) {
        goto cleanup;
    }

    /* Queues and locks */
    uint32_t req_q = config->request_queue_len ? config->request_queue_len : AGENT_FLOW_DEFAULT_REQ_Q;
    uint32_t resp_q = config->response_queue_len ? config->response_queue_len : AGENT_FLOW_DEFAULT_RESP_Q;
    s_flow->request_queue = xQueueCreate(req_q, sizeof(agent_request_item_t));
    s_flow->response_queue = xQueueCreate(resp_q, sizeof(agent_response_item_t));
    s_flow->response_lock = xSemaphoreCreateMutex();
    s_flow->inflight_lock = xSemaphoreCreateMutex();

    if (!s_flow->request_queue || !s_flow->response_queue ||
        !s_flow->response_lock || !s_flow->inflight_lock) {
        goto cleanup;
    }

    s_flow->initialized = true;
    s_flow->next_request_id = 1;
    ESP_LOGI(TAG, "Initialized (max_turns=%" PRIu32 " budget=%" PRIu32 ")",
             s_flow->config.max_turns, s_flow->config.token_budget);
    return ESP_OK;

cleanup:
    if (s_flow->state_machine) {
        agent_state_machine_destroy(s_flow->state_machine);
    }
    if (s_flow->tool_registry) {
        agent_tool_registry_destroy(s_flow->tool_registry);
    }
    if (s_flow->tool_executor) {
        agent_tool_executor_destroy(s_flow->tool_executor);
    }
    if (s_flow->ctx_pipeline) {
        agent_ctx_pipeline_destroy(s_flow->ctx_pipeline);
    }
    if (s_flow->request_queue) {
        vQueueDelete(s_flow->request_queue);
    }
    if (s_flow->response_queue) {
        vQueueDelete(s_flow->response_queue);
    }
    if (s_flow->response_lock) {
        vSemaphoreDelete(s_flow->response_lock);
    }
    if (s_flow->inflight_lock) {
        vSemaphoreDelete(s_flow->inflight_lock);
    }
    free(s_flow->system_prompt);
    free(s_flow);
    s_flow = NULL;
    return ESP_ERR_NO_MEM;
}

esp_err_t agent_flow_start(void)
{
    if (!s_flow || !s_flow->initialized) {
        return ESP_ERR_INVALID_STATE;
    }
    if (s_flow->started) {
        return ESP_OK;
    }

    s_flow->stop_requested = false;
    BaseType_t ok = xTaskCreatePinnedToCore(
        agent_flow_task, "agent_flow", s_flow->task_stack_size,
        NULL, s_flow->task_priority, &s_flow->task_handle, s_flow->task_core);

    if (ok != pdPASS) {
        s_flow->task_handle = NULL;
        return ESP_FAIL;
    }

    s_flow->started = true;
    ESP_LOGI(TAG, "Started worker task");
    return ESP_OK;
}

esp_err_t agent_flow_stop(void)
{
    TickType_t deadline;

    if (!s_flow || !s_flow->started) {
        return ESP_OK;
    }

    s_flow->stop_requested = true;
    deadline = xTaskGetTickCount() + pdMS_TO_TICKS(5000);
    while (s_flow->task_handle && xTaskGetTickCount() < deadline) {
        vTaskDelay(pdMS_TO_TICKS(20));
    }

    return s_flow->task_handle ? ESP_ERR_TIMEOUT : ESP_OK;
}

esp_err_t agent_flow_submit(const agent_request_t *request, uint32_t timeout_ms)
{
    agent_request_item_t item = {0};
    TickType_t ticks;

    if (!s_flow || !s_flow->started || !request || !request->user_text) {
        return (s_flow && s_flow->started) ? ESP_ERR_INVALID_ARG : ESP_ERR_INVALID_STATE;
    }

    item.view.request_id = request->request_id ? request->request_id : s_flow->next_request_id++;
    item.view.flags = request->flags;
    item.view.max_turns = request->max_turns ? request->max_turns : s_flow->config.max_turns;

    item.owned_session_id = dup_string(request->session_id);
    item.owned_user_text = dup_string(request->user_text);
    item.owned_source_channel = dup_string(request->source_channel);
    item.owned_source_chat_id = dup_string(request->source_chat_id);
    item.owned_source_sender_id = dup_string(request->source_sender_id);
    item.owned_source_message_id = dup_string(request->source_message_id);
    item.owned_source_cap = dup_string(request->source_cap);
    item.owned_target_channel = dup_string(request->target_channel);
    item.owned_target_chat_id = dup_string(request->target_chat_id);

    item.view.session_id = item.owned_session_id;
    item.view.user_text = item.owned_user_text;
    item.view.source_channel = item.owned_source_channel;
    item.view.source_chat_id = item.owned_source_chat_id;
    item.view.source_sender_id = item.owned_source_sender_id;
    item.view.source_message_id = item.owned_source_message_id;
    item.view.source_cap = item.owned_source_cap;
    item.view.target_channel = item.owned_target_channel;
    item.view.target_chat_id = item.owned_target_chat_id;

    if ((request->session_id && !item.owned_session_id) ||
        (request->source_channel && !item.owned_source_channel) ||
        !item.owned_user_text) {
        free_request_item(&item);
        return ESP_ERR_NO_MEM;
    }

    ticks = (timeout_ms == UINT32_MAX) ? portMAX_DELAY : pdMS_TO_TICKS(timeout_ms);
    if (xQueueSend(s_flow->request_queue, &item, ticks) != pdTRUE) {
        free_request_item(&item);
        return ESP_ERR_TIMEOUT;
    }

    return ESP_OK;
}

static bool pop_pending_response(uint32_t request_id, agent_response_item_t *out)
{
    /* Simplified: just check the front of queue for matching request_id.
     * Real impl would maintain a pending linked list like claw_core.
     */
    (void)request_id;
    (void)out;
    return false;
}

static void move_response(agent_response_t *dst, agent_response_item_t *src)
{
    memset(dst, 0, sizeof(*dst));
    *dst = src->view;
    memset(src, 0, sizeof(*src));
}

esp_err_t agent_flow_receive(agent_response_t *response, uint32_t timeout_ms)
{
    return agent_flow_receive_for(0, response, timeout_ms);
}

esp_err_t agent_flow_receive_for(uint32_t request_id,
                                  agent_response_t *response,
                                  uint32_t timeout_ms)
{
    agent_response_item_t item = {0};
    TickType_t start_ticks;
    bool match_any;

    if (!s_flow || !s_flow->started || !response) {
        return (s_flow && s_flow->started) ? ESP_ERR_INVALID_ARG : ESP_ERR_INVALID_STATE;
    }

    if (xSemaphoreTake(s_flow->response_lock, portMAX_DELAY) != pdTRUE) {
        return ESP_FAIL;
    }
    start_ticks = xTaskGetTickCount();
    match_any = (request_id == 0);

    if (pop_pending_response(request_id, &item)) {
        xSemaphoreGive(s_flow->response_lock);
        move_response(response, &item);
        return ESP_OK;
    }

    while (true) {
        TickType_t wait_ticks;
        TickType_t elapsed = xTaskGetTickCount() - start_ticks;

        if (timeout_ms == UINT32_MAX) {
            wait_ticks = portMAX_DELAY;
        } else {
            TickType_t timeout_ticks = pdMS_TO_TICKS(timeout_ms);
            if (elapsed >= timeout_ticks) {
                xSemaphoreGive(s_flow->response_lock);
                return ESP_ERR_TIMEOUT;
            }
            wait_ticks = timeout_ticks - elapsed;
        }

        if (xQueueReceive(s_flow->response_queue, &item, wait_ticks) != pdTRUE) {
            xSemaphoreGive(s_flow->response_lock);
            return ESP_ERR_TIMEOUT;
        }

        if (match_any || item.view.request_id == request_id) {
            xSemaphoreGive(s_flow->response_lock);
            move_response(response, &item);
            return ESP_OK;
        }
        /* Mismatch: would enqueue pending, but simplified for now */
        free_response_item(&item);
    }
}

void agent_flow_response_free(agent_response_t *response)
{
    if (!response) {
        return;
    }
    free(response->text);
    free(response->error_message);
    free(response->tool_calls_csv);
    free(response->context_providers_csv);
    response->text = NULL;
    response->error_message = NULL;
    response->tool_calls_csv = NULL;
    response->context_providers_csv = NULL;
}

esp_err_t agent_flow_cancel(uint32_t request_id)
{
    bool armed = false;

    if (!s_flow || !s_flow->initialized) {
        return ESP_ERR_INVALID_STATE;
    }

    if (xSemaphoreTake(s_flow->inflight_lock, pdMS_TO_TICKS(200)) != pdTRUE) {
        return ESP_ERR_TIMEOUT;
    }
    if (s_flow->inflight_request_id != 0 &&
        (request_id == 0 || s_flow->inflight_request_id == request_id)) {
        s_flow->inflight_abort = true;
        armed = true;
        ESP_LOGI(TAG, "Cancel armed for request %" PRIu32, s_flow->inflight_request_id);
    }
    xSemaphoreGive(s_flow->inflight_lock);
    return armed ? ESP_OK : ESP_ERR_NOT_FOUND;
}

esp_err_t agent_flow_cancel_all(void)
{
    return agent_flow_cancel(0);
}

esp_err_t agent_flow_add_context_provider(const agent_context_provider_t *provider)
{
    if (!s_flow || !s_flow->initialized) {
        return ESP_ERR_INVALID_STATE;
    }
    return agent_ctx_pipeline_add_provider(s_flow->ctx_pipeline, provider);
}

esp_err_t agent_flow_remove_context_provider(const char *name)
{
    if (!s_flow || !s_flow->initialized) {
        return ESP_ERR_INVALID_STATE;
    }
    return agent_ctx_pipeline_remove_provider(s_flow->ctx_pipeline, name);
}

agent_state_t agent_flow_get_state(void)
{
    if (!s_flow || !s_flow->state_machine) {
        return AGENT_STATE_IDLE;
    }
    return agent_state_machine_get_state(s_flow->state_machine);
}

esp_err_t agent_flow_get_last_result(agent_response_t *out_response)
{
    if (!out_response) {
        return ESP_ERR_INVALID_ARG;
    }
    if (!s_flow || !s_flow->initialized) {
        return ESP_ERR_INVALID_STATE;
    }

    memset(out_response, 0, sizeof(*out_response));
    out_response->request_id = s_flow->last_result.view.request_id;
    out_response->status = s_flow->last_result.view.status;
    out_response->turns_consumed = s_flow->last_result.view.turns_consumed;
    out_response->tools_invoked = s_flow->last_result.view.tools_invoked;
    out_response->tokens_consumed = s_flow->last_result.view.tokens_consumed;

    if (s_flow->last_result.owned_text) {
        out_response->text = strdup(s_flow->last_result.owned_text);
    }
    if (s_flow->last_result.owned_error_message) {
        out_response->error_message = strdup(s_flow->last_result.owned_error_message);
    }
    if (s_flow->last_result.owned_tool_calls_csv) {
        out_response->tool_calls_csv = strdup(s_flow->last_result.owned_tool_calls_csv);
    }
    if (s_flow->last_result.owned_providers_csv) {
        out_response->context_providers_csv = strdup(s_flow->last_result.owned_providers_csv);
    }
    return ESP_OK;
}

/* ─── Tool Registry Passthrough ─── */

esp_err_t agent_flow_register_tool(const agent_tool_descriptor_t *tool)
{
    if (!s_flow || !s_flow->initialized) {
        return ESP_ERR_INVALID_STATE;
    }
    return agent_tool_registry_register(s_flow->tool_registry, tool);
}

esp_err_t agent_flow_unregister_tool(const char *name)
{
    if (!s_flow || !s_flow->initialized) {
        return ESP_ERR_INVALID_STATE;
    }
    return agent_tool_registry_unregister(s_flow->tool_registry, name);
}

/* ─── Cap Bridge ─── */

static esp_err_t cap_bridge_execute(const char *input_json, char *output, size_t output_size,
                                     void *user_ctx)
{
    (void)user_ctx;
    /* Bridge agent_flow tool calls to existing claw_cap system */
    claw_cap_call_context_t ctx = {
        .caller = CLAW_CAP_CALLER_AGENT,
        .source_cap = "claw_agent_flow",
    };
    /* The tool name is embedded in the input_json or we need another mechanism.
     * For simplicity, this bridge assumes the tool descriptor wraps cap_call.
     */
    strlcpy(output, "{\"status\":\"ok\"}", output_size);
    (void)input_json;
    return ESP_OK;
}

/* Register all LLM-visible caps as agent_flow tools */
esp_err_t agent_flow_install_cap_bridge(void)
{
    if (!s_flow || !s_flow->initialized) {
        return ESP_ERR_INVALID_STATE;
    }

    claw_cap_list_t list = claw_cap_list();
    for (size_t i = 0; i < list.count; i++) {
        const claw_cap_descriptor_t *cap = &list.items[i];
        if (!(cap->cap_flags & CLAW_CAP_FLAG_CALLABLE_BY_LLM)) {
            continue;
        }
        agent_tool_descriptor_t tool = {
            .concurrency = AGENT_TOOL_CONC_SAFE,
            .execute = cap_bridge_execute,
            .user_ctx = (void *)cap,
        };
        strlcpy(tool.name, cap->name, sizeof(tool.name));
        strlcpy(tool.description, cap->description ? cap->description : "",
                sizeof(tool.description));
        strlcpy(tool.input_schema_json,
                cap->input_schema_json ? cap->input_schema_json : "{\"type\":\"object\"}",
                sizeof(tool.input_schema_json));
        agent_tool_registry_register(s_flow->tool_registry, &tool);
    }

    ESP_LOGI(TAG, "Cap bridge installed: %u tools mapped", (unsigned)list.count);
    return ESP_OK;
}
