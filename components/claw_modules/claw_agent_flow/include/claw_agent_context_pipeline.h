/*
 * SPDX-FileCopyrightText: 2026 Espressif Systems (Shanghai) CO LTD
 *
 * SPDX-License-Identifier: Apache-2.0
 */
#pragma once

#include "claw_agent_flow.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Pipeline handle (opaque) */
typedef struct agent_ctx_pipeline agent_ctx_pipeline_t;

/* Create/destroy */
agent_ctx_pipeline_t *agent_ctx_pipeline_create(size_t max_providers, uint32_t token_budget);
void agent_ctx_pipeline_destroy(agent_ctx_pipeline_t *pipeline);

/* Provider management */
esp_err_t agent_ctx_pipeline_add_provider(agent_ctx_pipeline_t *pipeline,
                                           const agent_context_provider_t *provider);
esp_err_t agent_ctx_pipeline_remove_provider(agent_ctx_pipeline_t *pipeline,
                                              const char *name);

/* Build context for a request.
 * Collects from all providers, sorts by priority, assembles into
 * system prompt + messages + tools JSON.
 */
esp_err_t agent_ctx_pipeline_build(agent_ctx_pipeline_t *pipeline,
                                    const agent_request_t *req,
                                    char **out_system_prompt,
                                    char **out_messages_json,
                                    char **out_tools_json,
                                    uint32_t *out_token_estimate);

/* Token budget management */
esp_err_t agent_ctx_pipeline_set_budget(agent_ctx_pipeline_t *pipeline,
                                         uint32_t token_budget);
bool agent_ctx_pipeline_needs_compact(const agent_ctx_pipeline_t *pipeline,
                                       uint32_t threshold);

/* Compact: trim message history to fit budget */
esp_err_t agent_ctx_pipeline_compact(agent_ctx_pipeline_t *pipeline);

/* Utility: estimate token count (rough heuristic) */
uint32_t agent_ctx_estimate_tokens(const char *text);

#ifdef __cplusplus
}
#endif
