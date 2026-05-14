/*
 * SPDX-FileCopyrightText: 2026 Espressif Systems (Shanghai) CO LTD
 *
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * EXAMPLE: Integration of claw_agent_flow into app_claw.c
 *
 * This file demonstrates how to replace or augment the existing
 * claw_core_init/claw_core_start usage with the new agent_flow system.
 *
 * It is NOT compiled by default. Copy relevant sections into
 * components/common/app_claw/app_claw.c as desired.
 */

#include "app_claw.h"
#include "claw_agent_flow.h"
#include "claw_memory.h"
#include "claw_skill.h"
#include "claw_cap.h"
#include "esp_log.h"

static const char *TAG = "app_claw";

/* ======================================================================== */
/* Section 1: Context Providers                                            */
/* ======================================================================== */

/* Bridge memory providers into agent_flow context pipeline */
static esp_err_t ctxp_memory_profile(const agent_request_t *req,
                                      agent_context_item_t *out,
                                      void *ctx)
{
    (void)req; (void)ctx;
    /* Delegate to existing claw_memory provider */
    out->kind = AGENT_CTX_KIND_SYSTEM_PROMPT;
    out->priority = 80;
    strlcpy(out->provider_name, "memory_profile", sizeof(out->provider_name));
    /* In real integration, call claw_memory_profile_provider.collect() */
    out->content = strdup("User profile context (placeholder)");
    return out->content ? ESP_OK : ESP_ERR_NO_MEM;
}

static esp_err_t ctxp_memory_longterm(const agent_request_t *req,
                                       agent_context_item_t *out,
                                       void *ctx)
{
    (void)req; (void)ctx;
    out->kind = AGENT_CTX_KIND_SYSTEM_PROMPT;
    out->priority = 70;
    strlcpy(out->provider_name, "memory_longterm", sizeof(out->provider_name));
    out->content = strdup("Long-term memory context (placeholder)");
    return out->content ? ESP_OK : ESP_ERR_NO_MEM;
}

static esp_err_t ctxp_session_history(const agent_request_t *req,
                                       agent_context_item_t *out,
                                       void *ctx)
{
    (void)ctx;
    out->kind = AGENT_CTX_KIND_MESSAGE_HISTORY;
    out->priority = 90;
    strlcpy(out->provider_name, "session_history", sizeof(out->provider_name));
    /* Build message history from session_id */
    if (req->session_id && req->session_id[0]) {
        /* Real: claw_memory_session_history_provider.collect() */
        out->content = strdup("[{\"role\":\"system\",\"content\":\"history\"}]");
    } else {
        return ESP_ERR_NOT_FOUND;
    }
    return out->content ? ESP_OK : ESP_ERR_NO_MEM;
}

static esp_err_t ctxp_skills_list(const agent_request_t *req,
                                   agent_context_item_t *out,
                                   void *ctx)
{
    (void)req; (void)ctx;
    out->kind = AGENT_CTX_KIND_SYSTEM_PROMPT;
    out->priority = 60;
    strlcpy(out->provider_name, "skills", sizeof(out->provider_name));
    /* Real: claw_skill_skills_list_provider.collect() */
    out->content = strdup("## Available Skills\n- memory_ops\n- web_search\n");
    return out->content ? ESP_OK : ESP_ERR_NO_MEM;
}

static esp_err_t ctxp_cap_tools(const agent_request_t *req,
                                 agent_context_item_t *out,
                                 void *ctx)
{
    (void)req; (void)ctx;
    out->kind = AGENT_CTX_KIND_TOOL_DEF;
    out->priority = 50;
    strlcpy(out->provider_name, "tools", sizeof(out->provider_name));
    /* Build tools JSON from agent_flow registry */
    out->content = agent_tool_registry_build_tools_json(
        /* would need accessor; simplified */ NULL);
    return out->content ? ESP_OK : ESP_ERR_NO_MEM;
}

/* ======================================================================== */
/* Section 2: State Change Observer                                        */
/* ======================================================================== */

static void on_agent_state_change(agent_state_t old_state,
                                   agent_state_t new_state,
                                   agent_event_t event,
                                   void *user_ctx)
{
    (void)user_ctx;
    ESP_LOGI(TAG, "[AgentFlow] %s -> %s [%s]",
             agent_state_to_string(old_state),
             agent_state_to_string(new_state),
             agent_event_to_string(event));

#if CONFIG_APP_CLAW_ENABLE_EMOTE
    /* Optional: drive UI emote based on agent state */
    if (new_state == AGENT_STATE_LLM_CALL || new_state == AGENT_STATE_STREAMING) {
        /* emote_set_thinking(true); */
    } else if (new_state == AGENT_STATE_TOOL_EXECUTE) {
        /* emote_set_working(true); */
    } else if (new_state == AGENT_STATE_RESPONDING || new_state == AGENT_STATE_DONE) {
        /* emote_set_thinking(false); */
    }
#endif
}

/* ======================================================================== */
/* Section 3: Replace claw_core_init with agent_flow_init                  */
/* ======================================================================== */

static esp_err_t app_claw_init_agent_flow(const app_claw_config_t *config)
{
    agent_flow_config_t flow_cfg = {
        /* LLM */
        .api_key = config->llm_api_key,
        .backend_type = config->llm_backend_type,
        .model = config->llm_model,
        .base_url = config->llm_base_url,
        .auth_type = config->llm_auth_type,
        .max_tokens_field = config->llm_max_tokens_field,
        .timeout_ms = (uint32_t)strtoul(config->llm_timeout_ms, NULL, 10),
        .max_tokens = (uint32_t)strtoul(config->llm_max_tokens, NULL, 10),
        .image_max_bytes = (size_t)strtoul(config->llm_default_image_max_bytes, NULL, 10),
        .supports_tools = true,
        .supports_vision = true,
        .system_prompt = APP_SYSTEM_PROMPT,

        /* Query loop */
        .max_turns = 32,
        .token_budget = 8000,
        .compact_threshold = 7000,

        /* Tool orchestration */
        .max_concurrent_tools = 4,
        .enable_permission_checks = false,

        /* Hooks */
        .on_state_change = on_agent_state_change,
        .on_permission = NULL, /* could implement 6-tier rule system here */

        /* Task */
        .task_stack_size = 16 * 1024,
        .task_priority = 5,
        .task_core = tskNO_AFFINITY,
    };

    ESP_RETURN_ON_ERROR(agent_flow_init(&flow_cfg), TAG, "agent_flow_init failed");

    /* Install bridge: all LLM-visible caps become agent_flow tools */
    ESP_RETURN_ON_ERROR(agent_flow_install_cap_bridge(), TAG, "cap_bridge failed");

    /* Register context providers (priority order) */
    agent_flow_add_context_provider(&(agent_context_provider_t){
        .name = "session_history",
        .kind = AGENT_CTX_KIND_MESSAGE_HISTORY,
        .collect = ctxp_session_history,
        .priority = 90,
    });
    agent_flow_add_context_provider(&(agent_context_provider_t){
        .name = "memory_profile",
        .kind = AGENT_CTX_KIND_SYSTEM_PROMPT,
        .collect = ctxp_memory_profile,
        .priority = 80,
    });
    agent_flow_add_context_provider(&(agent_context_provider_t){
        .name = "memory_longterm",
        .kind = AGENT_CTX_KIND_SYSTEM_PROMPT,
        .collect = ctxp_memory_longterm,
        .priority = 70,
    });
    agent_flow_add_context_provider(&(agent_context_provider_t){
        .name = "skills",
        .kind = AGENT_CTX_KIND_SYSTEM_PROMPT,
        .collect = ctxp_skills_list,
        .priority = 60,
    });
    agent_flow_add_context_provider(&(agent_context_provider_t){
        .name = "tools",
        .kind = AGENT_CTX_KIND_TOOL_DEF,
        .collect = ctxp_cap_tools,
        .priority = 50,
    });

    ESP_RETURN_ON_ERROR(agent_flow_start(), TAG, "agent_flow_start failed");
    return ESP_OK;
}

/* ======================================================================== */
/* Section 4: Event Router Integration                                     */
/* ======================================================================== */

/*
 * The event router can submit requests directly to agent_flow instead of
 * claw_core. Add this action handler in the router rule or as a default:
 */

static esp_err_t route_event_to_agent_flow(const claw_event_t *event)
{
    agent_request_t req = {
        .request_id = 0, /* auto-assign */
        .flags = AGENT_REQ_FLAG_PUBLISH_OUT_MSG,
        .session_id = event->chat_id,
        .user_text = event->text,
        .source_channel = event->source_channel,
        .source_chat_id = event->chat_id,
        .source_sender_id = event->sender_id,
        .source_message_id = event->message_id,
        .source_cap = event->source_cap,
    };
    return agent_flow_submit(&req, 1000);
}

/* ======================================================================== */
/* Section 5: Migration Path                                               */
/* ======================================================================== */

/*
 * Phase 1 (current): Add agent_flow as parallel system.
 *   - Keep claw_core running for existing routes
 *   - Route new events to agent_flow via optional flag
 *
 * Phase 2: Migrate all routes to agent_flow.
 *   - Remove claw_core dependency
 *   - Replace claw_core_submit with agent_flow_submit
 *
 * Phase 3: Retire claw_core.
 *   - Move claw_core_llm transport into agent_flow
 *   - Remove claw_core component
 */
