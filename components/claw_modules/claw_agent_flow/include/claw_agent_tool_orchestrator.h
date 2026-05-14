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

/* Tool registry handle (opaque) */
typedef struct agent_tool_registry agent_tool_registry_t;

/* Batch executor handle (opaque) */
typedef struct agent_tool_executor agent_tool_executor_t;

/* ─── Tool Registry ─── */

agent_tool_registry_t *agent_tool_registry_create(size_t max_tools);
void agent_tool_registry_destroy(agent_tool_registry_t *reg);

esp_err_t agent_tool_registry_register(agent_tool_registry_t *reg,
                                        const agent_tool_descriptor_t *tool);
esp_err_t agent_tool_registry_unregister(agent_tool_registry_t *reg,
                                          const char *name);
const agent_tool_descriptor_t *agent_tool_registry_find(agent_tool_registry_t *reg,
                                                         const char *name);

/* Build tools JSON for LLM function calling */
char *agent_tool_registry_build_tools_json(agent_tool_registry_t *reg);

/* ─── Batch Executor ─── */

typedef struct {
    uint32_t max_concurrent;
    uint32_t timeout_ms_per_tool;
    agent_hook_permission_fn permission_fn;
    void *permission_ctx;
    agent_hook_pre_tool_fn pre_tool_fn;
    void *pre_tool_ctx;
    agent_hook_post_tool_fn post_tool_fn;
    void *post_tool_ctx;
} agent_tool_executor_config_t;

agent_tool_executor_t *agent_tool_executor_create(const agent_tool_executor_config_t *config);
void agent_tool_executor_destroy(agent_tool_executor_t *exec);

/* Execute a batch of tool calls.
 * This function blocks until all tools complete.
 * Tools marked CONC_SAFE run in parallel; CONC_EXCLUSIVE run serially.
 */
esp_err_t agent_tool_executor_run_batch(agent_tool_executor_t *exec,
                                         agent_tool_registry_t *reg,
                                         agent_tool_batch_t *batch);

/* ─── Permission helper ─── */

agent_tool_permission_t agent_tool_check_permission_default(const char *tool_name,
                                                             const char *arguments_json);

#ifdef __cplusplus
}
#endif
