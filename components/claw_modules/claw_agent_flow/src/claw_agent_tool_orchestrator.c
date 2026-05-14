/*
 * SPDX-FileCopyrightText: 2026 Espressif Systems (Shanghai) CO LTD
 *
 * SPDX-License-Identifier: Apache-2.0
 */
#include "claw_agent_tool_orchestrator.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "cJSON.h"
#include "esp_log.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "freertos/task.h"

static const char *TAG = "agent_tool";

struct agent_tool_registry {
    agent_tool_descriptor_t *tools;
    size_t count;
    size_t capacity;
    SemaphoreHandle_t lock;
};

/* ─── Tool Registry ─── */

agent_tool_registry_t *agent_tool_registry_create(size_t max_tools)
{
    agent_tool_registry_t *reg = calloc(1, sizeof(*reg));
    if (!reg) {
        return NULL;
    }
    reg->tools = calloc(max_tools, sizeof(*reg->tools));
    if (!reg->tools) {
        free(reg);
        return NULL;
    }
    reg->capacity = max_tools;
    reg->count = 0;
    reg->lock = xSemaphoreCreateMutex();
    if (!reg->lock) {
        free(reg->tools);
        free(reg);
        return NULL;
    }
    return reg;
}

void agent_tool_registry_destroy(agent_tool_registry_t *reg)
{
    if (!reg) {
        return;
    }
    if (reg->lock) {
        vSemaphoreDelete(reg->lock);
    }
    free(reg->tools);
    free(reg);
}

esp_err_t agent_tool_registry_register(agent_tool_registry_t *reg,
                                        const agent_tool_descriptor_t *tool)
{
    if (!reg || !tool || !tool->name[0]) {
        return ESP_ERR_INVALID_ARG;
    }

    xSemaphoreTake(reg->lock, portMAX_DELAY);
    for (size_t i = 0; i < reg->count; i++) {
        if (strcmp(reg->tools[i].name, tool->name) == 0) {
            xSemaphoreGive(reg->lock);
            return ESP_ERR_INVALID_STATE;
        }
    }
    if (reg->count >= reg->capacity) {
        xSemaphoreGive(reg->lock);
        return ESP_ERR_NO_MEM;
    }
    reg->tools[reg->count++] = *tool;
    xSemaphoreGive(reg->lock);

    ESP_LOGI(TAG, "Registered tool: %s (concurrency=%s)",
             tool->name,
             tool->concurrency == AGENT_TOOL_CONC_SAFE ? "safe" : "exclusive");
    return ESP_OK;
}

esp_err_t agent_tool_registry_unregister(agent_tool_registry_t *reg, const char *name)
{
    if (!reg || !name) {
        return ESP_ERR_INVALID_ARG;
    }

    xSemaphoreTake(reg->lock, portMAX_DELAY);
    for (size_t i = 0; i < reg->count; i++) {
        if (strcmp(reg->tools[i].name, name) == 0) {
            memmove(&reg->tools[i], &reg->tools[i + 1],
                    (reg->count - i - 1) * sizeof(*reg->tools));
            reg->count--;
            xSemaphoreGive(reg->lock);
            return ESP_OK;
        }
    }
    xSemaphoreGive(reg->lock);
    return ESP_ERR_NOT_FOUND;
}

const agent_tool_descriptor_t *agent_tool_registry_find(agent_tool_registry_t *reg,
                                                         const char *name)
{
    if (!reg || !name) {
        return NULL;
    }

    xSemaphoreTake(reg->lock, portMAX_DELAY);
    for (size_t i = 0; i < reg->count; i++) {
        if (strcmp(reg->tools[i].name, name) == 0) {
            const agent_tool_descriptor_t *found = &reg->tools[i];
            xSemaphoreGive(reg->lock);
            return found;
        }
    }
    xSemaphoreGive(reg->lock);
    return NULL;
}

char *agent_tool_registry_build_tools_json(agent_tool_registry_t *reg)
{
    cJSON *tools = NULL;
    char *json = NULL;

    if (!reg) {
        return NULL;
    }

    tools = cJSON_CreateArray();
    if (!tools) {
        return NULL;
    }

    xSemaphoreTake(reg->lock, portMAX_DELAY);
    for (size_t i = 0; i < reg->count; i++) {
        cJSON *tool = cJSON_CreateObject();
        cJSON *function = cJSON_CreateObject();
        cJSON *parameters = cJSON_Parse(reg->tools[i].input_schema_json);

        if (!tool || !function) {
            cJSON_Delete(tool);
            cJSON_Delete(function);
            cJSON_Delete(parameters);
            continue;
        }

        cJSON_AddStringToObject(function, "name", reg->tools[i].name);
        cJSON_AddStringToObject(function, "description", reg->tools[i].description);
        if (parameters && cJSON_IsObject(parameters)) {
            cJSON_AddItemToObject(function, "parameters", parameters);
        } else {
            cJSON_Delete(parameters);
            cJSON *empty_params = cJSON_CreateObject();
            cJSON_AddStringToObject(empty_params, "type", "object");
            cJSON_AddItemToObject(function, "parameters", empty_params);
        }
        cJSON_AddStringToObject(tool, "type", "function");
        cJSON_AddItemToObject(tool, "function", function);
        cJSON_AddItemToArray(tools, tool);
    }
    xSemaphoreGive(reg->lock);

    json = cJSON_PrintUnformatted(tools);
    cJSON_Delete(tools);
    return json;
}

/* ─── Batch Executor ─── */

struct agent_tool_executor {
    agent_tool_executor_config_t config;
    SemaphoreHandle_t exclusive_lock;
};

agent_tool_executor_t *agent_tool_executor_create(const agent_tool_executor_config_t *config)
{
    agent_tool_executor_t *exec = calloc(1, sizeof(*exec));
    if (!exec) {
        return NULL;
    }
    if (config) {
        exec->config = *config;
    }
    exec->exclusive_lock = xSemaphoreCreateMutex();
    if (!exec->exclusive_lock) {
        free(exec);
        return NULL;
    }
    return exec;
}

void agent_tool_executor_destroy(agent_tool_executor_t *exec)
{
    if (!exec) {
        return;
    }
    if (exec->exclusive_lock) {
        vSemaphoreDelete(exec->exclusive_lock);
    }
    free(exec);
}

static esp_err_t execute_single_tool(agent_tool_executor_t *exec,
                                      agent_tool_registry_t *reg,
                                      agent_tool_call_t *call)
{
    const agent_tool_descriptor_t *desc = agent_tool_registry_find(reg, call->name);
    if (!desc) {
        call->phase = AGENT_TOOL_PHASE_FAILED;
        call->error = ESP_ERR_NOT_FOUND;
        snprintf(call->result_json, sizeof(call->result_json),
                 "{\"error\":\"Tool '%s' not found\"}", call->name);
        return ESP_ERR_NOT_FOUND;
    }

    /* Permission check */
    if (exec->config.permission_fn) {
        agent_tool_permission_t perm = AGENT_TOOL_PERM_ALLOW;
        esp_err_t err = exec->config.permission_fn(call->name, call->arguments_json,
                                                    &perm, exec->config.permission_ctx);
        if (err != ESP_OK || perm == AGENT_TOOL_PERM_DENY) {
            call->phase = AGENT_TOOL_PHASE_DENIED;
            call->error = ESP_ERR_NOT_ALLOWED;
            snprintf(call->result_json, sizeof(call->result_json),
                     "{\"error\":\"Permission denied for '%s'\"}", call->name);
            ESP_LOGW(TAG, "Tool denied: %s", call->name);
            return ESP_ERR_NOT_ALLOWED;
        }
    }

    /* Pre-tool hook */
    if (exec->config.pre_tool_fn) {
        exec->config.pre_tool_fn(call, exec->config.pre_tool_ctx);
    }

    /* Acquire concurrency lock if exclusive */
    if (desc->concurrency == AGENT_TOOL_CONC_EXCLUSIVE) {
        xSemaphoreTake(exec->exclusive_lock, portMAX_DELAY);
    }

    call->phase = AGENT_TOOL_PHASE_EXECUTING;
    TickType_t start = xTaskGetTickCount();

    char output[sizeof(call->result_json)] = {0};
    esp_err_t err = desc->execute(call->arguments_json, output, sizeof(output), desc->user_ctx);

    call->duration_ms = (uint32_t)(xTaskGetTickCount() - start) * portTICK_PERIOD_MS;
    call->error = err;

    if (desc->concurrency == AGENT_TOOL_CONC_EXCLUSIVE) {
        xSemaphoreGive(exec->exclusive_lock);
    }

    if (err == ESP_OK) {
        call->phase = AGENT_TOOL_PHASE_DONE;
        strlcpy(call->result_json, output, sizeof(call->result_json));
    } else {
        call->phase = AGENT_TOOL_PHASE_FAILED;
        snprintf(call->result_json, sizeof(call->result_json),
                 "{\"error\":\"%s\"}", esp_err_to_name(err));
    }

    /* Post-tool hook */
    if (exec->config.post_tool_fn) {
        exec->config.post_tool_fn(call, exec->config.post_tool_ctx);
    }

    ESP_LOGI(TAG, "Tool %s done: err=%s duration=%u ms",
             call->name, esp_err_to_name(err), (unsigned)call->duration_ms);
    return err;
}

/*
 * Execute batch with simple two-phase strategy:
 *   Phase 1: Run all CONC_SAFE tools in parallel (one task each)
 *   Phase 2: Run all CONC_EXCLUSIVE tools serially
 *
 * On embedded FreeRTOS, "parallel" means spawning tasks for each tool.
 * To keep stack usage bounded, we limit concurrent tasks.
 */

typedef struct {
    agent_tool_executor_t *exec;
    agent_tool_registry_t *reg;
    agent_tool_call_t *call;
} tool_task_ctx_t;

static void tool_task_fn(void *arg)
{
    tool_task_ctx_t *ctx = arg;
    execute_single_tool(ctx->exec, ctx->reg, ctx->call);
    free(ctx);
    vTaskDelete(NULL);
}

esp_err_t agent_tool_executor_run_batch(agent_tool_executor_t *exec,
                                         agent_tool_registry_t *reg,
                                         agent_tool_batch_t *batch)
{
    if (!exec || !reg || !batch) {
        return ESP_ERR_INVALID_ARG;
    }

    /* Phase 1: concurrent-safe tools */
    size_t concurrent_count = 0;
    TaskHandle_t *handles = NULL;
    tool_task_ctx_t **ctxs = NULL;

    for (size_t i = 0; i < batch->count; i++) {
        if (batch->items[i].concurrency == AGENT_TOOL_CONC_SAFE &&
            batch->items[i].phase == AGENT_TOOL_PHASE_PENDING) {
            concurrent_count++;
        }
    }

    if (concurrent_count > 0) {
        handles = calloc(concurrent_count, sizeof(*handles));
        ctxs = calloc(concurrent_count, sizeof(*ctxs));
        if (!handles || !ctxs) {
            free(handles);
            free(ctxs);
            return ESP_ERR_NO_MEM;
        }

        size_t idx = 0;
        for (size_t i = 0; i < batch->count && idx < concurrent_count; i++) {
            if (batch->items[i].concurrency != AGENT_TOOL_CONC_SAFE ||
                batch->items[i].phase != AGENT_TOOL_PHASE_PENDING) {
                continue;
            }

            ctxs[idx] = malloc(sizeof(*ctxs[idx]));
            if (!ctxs[idx]) {
                continue;
            }
            ctxs[idx]->exec = exec;
            ctxs[idx]->reg = reg;
            ctxs[idx]->call = &batch->items[i];
            batch->items[i].phase = AGENT_TOOL_PHASE_DISPATCHED;

            char task_name[32];
            snprintf(task_name, sizeof(task_name), "tool_%s", batch->items[i].name);
            /* Truncate to fit FreeRTOS name limit */
            task_name[configMAX_TASK_NAME_LEN - 1] = '\0';

            BaseType_t ok = xTaskCreatePinnedToCore(
                tool_task_fn, task_name, 4096, ctxs[idx], 5, &handles[idx], tskNO_AFFINITY);
            if (ok != pdPASS) {
                ESP_LOGW(TAG, "Failed to spawn task for tool %s", batch->items[i].name);
                free(ctxs[idx]);
                handles[idx] = NULL;
            }
            idx++;
        }

        /* Wait for all concurrent tasks */
        for (size_t i = 0; i < concurrent_count; i++) {
            if (handles[i]) {
                /* Poll with timeout */
                uint32_t wait_ms = 0;
                while (eTaskGetState(handles[i]) != eDeleted && wait_ms < 60000) {
                    vTaskDelay(pdMS_TO_TICKS(10));
                    wait_ms += 10;
                }
            }
        }
        free(handles);
        free(ctxs);
    }

    /* Phase 2: exclusive tools — run serially */
    for (size_t i = 0; i < batch->count; i++) {
        if (batch->items[i].concurrency == AGENT_TOOL_CONC_EXCLUSIVE &&
            batch->items[i].phase == AGENT_TOOL_PHASE_PENDING) {
            batch->items[i].phase = AGENT_TOOL_PHASE_DISPATCHED;
            execute_single_tool(exec, reg, &batch->items[i]);
        }
    }

    return ESP_OK;
}

agent_tool_permission_t agent_tool_check_permission_default(const char *tool_name,
                                                             const char *arguments_json)
{
    (void)arguments_json;
    (void)tool_name;
    return AGENT_TOOL_PERM_ALLOW;
}
