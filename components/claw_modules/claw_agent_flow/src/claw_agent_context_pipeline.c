/*
 * SPDX-FileCopyrightText: 2026 Espressif Systems (Shanghai) CO LTD
 *
 * SPDX-License-Identifier: Apache-2.0
 */
#include "claw_agent_context_pipeline.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "cJSON.h"
#include "esp_log.h"
#include "freertos/semphr.h"

static const char *TAG = "agent_ctx";

#define AGENT_CTX_MAX_CONTENT_LEN (4096)

struct agent_ctx_pipeline {
    agent_context_provider_t *providers;
    size_t provider_count;
    size_t provider_capacity;
    uint32_t token_budget;
    SemaphoreHandle_t lock;
};

agent_ctx_pipeline_t *agent_ctx_pipeline_create(size_t max_providers, uint32_t token_budget)
{
    agent_ctx_pipeline_t *pipeline = calloc(1, sizeof(*pipeline));
    if (!pipeline) {
        return NULL;
    }
    pipeline->providers = calloc(max_providers, sizeof(*pipeline->providers));
    if (!pipeline->providers) {
        free(pipeline);
        return NULL;
    }
    pipeline->provider_capacity = max_providers;
    pipeline->provider_count = 0;
    pipeline->token_budget = token_budget;
    pipeline->lock = xSemaphoreCreateMutex();
    if (!pipeline->lock) {
        free(pipeline->providers);
        free(pipeline);
        return NULL;
    }
    return pipeline;
}

void agent_ctx_pipeline_destroy(agent_ctx_pipeline_t *pipeline)
{
    if (!pipeline) {
        return;
    }
    if (pipeline->lock) {
        vSemaphoreDelete(pipeline->lock);
    }
    for (size_t i = 0; i < pipeline->provider_count; i++) {
        free(pipeline->providers[i].user_ctx);
    }
    free(pipeline->providers);
    free(pipeline);
}

static int provider_cmp(const void *a, const void *b)
{
    const agent_context_provider_t *pa = a;
    const agent_context_provider_t *pb = b;
    /* Higher priority first */
    return (int)pb->priority - (int)pa->priority;
}

esp_err_t agent_ctx_pipeline_add_provider(agent_ctx_pipeline_t *pipeline,
                                           const agent_context_provider_t *provider)
{
    if (!pipeline || !provider || !provider->name[0] || !provider->collect) {
        return ESP_ERR_INVALID_ARG;
    }

    xSemaphoreTake(pipeline->lock, portMAX_DELAY);
    if (pipeline->provider_count >= pipeline->provider_capacity) {
        xSemaphoreGive(pipeline->lock);
        return ESP_ERR_NO_MEM;
    }
    pipeline->providers[pipeline->provider_count++] = *provider;
    qsort(pipeline->providers, pipeline->provider_count, sizeof(*pipeline->providers), provider_cmp);
    xSemaphoreGive(pipeline->lock);

    ESP_LOGI(TAG, "Added context provider: %s (kind=%d, prio=%u)",
             provider->name, provider->kind, (unsigned)provider->priority);
    return ESP_OK;
}

esp_err_t agent_ctx_pipeline_remove_provider(agent_ctx_pipeline_t *pipeline,
                                              const char *name)
{
    if (!pipeline || !name) {
        return ESP_ERR_INVALID_ARG;
    }

    xSemaphoreTake(pipeline->lock, portMAX_DELAY);
    for (size_t i = 0; i < pipeline->provider_count; i++) {
        if (strcmp(pipeline->providers[i].name, name) == 0) {
            memmove(&pipeline->providers[i], &pipeline->providers[i + 1],
                    (pipeline->provider_count - i - 1) * sizeof(*pipeline->providers));
            pipeline->provider_count--;
            xSemaphoreGive(pipeline->lock);
            return ESP_OK;
        }
    }
    xSemaphoreGive(pipeline->lock);
    return ESP_ERR_NOT_FOUND;
}

uint32_t agent_ctx_estimate_tokens(const char *text)
{
    if (!text || !text[0]) {
        return 0;
    }
    /* Rough heuristic: ~4 chars per token for English/ASCII,
     * ~2 chars per token for CJK. Use conservative estimate.
     */
    size_t len = strlen(text);
    size_t ascii = 0;
    size_t non_ascii = 0;
    for (size_t i = 0; i < len; i++) {
        if ((unsigned char)text[i] < 0x80) {
            ascii++;
        } else {
            non_ascii++;
        }
    }
    return (uint32_t)(ascii / 4 + non_ascii / 2 + 1);
}

esp_err_t agent_ctx_pipeline_build(agent_ctx_pipeline_t *pipeline,
                                    const agent_request_t *req,
                                    char **out_system_prompt,
                                    char **out_messages_json,
                                    char **out_tools_json,
                                    uint32_t *out_token_estimate)
{
    if (!pipeline || !req || !out_system_prompt || !out_messages_json) {
        return ESP_ERR_INVALID_ARG;
    }

    *out_system_prompt = NULL;
    *out_messages_json = NULL;
    if (out_tools_json) {
        *out_tools_json = NULL;
    }
    if (out_token_estimate) {
        *out_token_estimate = 0;
    }

    cJSON *messages = cJSON_CreateArray();
    char *system_prompt = calloc(1, AGENT_CTX_MAX_CONTENT_LEN);
    if (!messages || !system_prompt) {
        cJSON_Delete(messages);
        free(system_prompt);
        return ESP_ERR_NO_MEM;
    }

    uint32_t total_tokens = 0;

    xSemaphoreTake(pipeline->lock, portMAX_DELAY);

    for (size_t i = 0; i < pipeline->provider_count; i++) {
        agent_context_item_t item = {0};
        esp_err_t err = pipeline->providers[i].collect(req, &item, pipeline->providers[i].user_ctx);
        if (err == ESP_ERR_NOT_FOUND) {
            continue;
        }
        if (err != ESP_OK) {
            ESP_LOGW(TAG, "Provider %s failed: %s",
                     pipeline->providers[i].name, esp_err_to_name(err));
            continue;
        }
        if (!item.content || !item.content[0]) {
            free(item.content);
            continue;
        }

        uint32_t item_tokens = agent_ctx_estimate_tokens(item.content);
        if (total_tokens + item_tokens > pipeline->token_budget) {
            ESP_LOGW(TAG, "Provider %s skipped: would exceed token budget", pipeline->providers[i].name);
            free(item.content);
            continue;
        }
        total_tokens += item_tokens;

        switch (item.kind) {
        case AGENT_CTX_KIND_SYSTEM_PROMPT:
            if (system_prompt[0]) {
                strlcat(system_prompt, "\n\n", AGENT_CTX_MAX_CONTENT_LEN);
            }
            strlcat(system_prompt, item.content, AGENT_CTX_MAX_CONTENT_LEN);
            break;

        case AGENT_CTX_KIND_MESSAGE_HISTORY:
        case AGENT_CTX_KIND_MEMORY:
        case AGENT_CTX_KIND_CURRENT_TURN: {
            cJSON *parsed = cJSON_Parse(item.content);
            if (parsed && cJSON_IsArray(parsed)) {
                const cJSON *msg = NULL;
                cJSON_ArrayForEach(msg, parsed) {
                    cJSON *dup = cJSON_Duplicate(msg, true);
                    if (dup) {
                        cJSON_AddItemToArray(messages, dup);
                    }
                }
            } else if (parsed && cJSON_IsObject(parsed)) {
                cJSON_AddItemToArray(messages, parsed);
                parsed = NULL; /* ownership transferred */
            }
            cJSON_Delete(parsed);
            break;
        }

        case AGENT_CTX_KIND_SKILL:
            /* Skills inject into system prompt */
            if (system_prompt[0]) {
                strlcat(system_prompt, "\n\n## Skill: ", AGENT_CTX_MAX_CONTENT_LEN);
            } else {
                strlcat(system_prompt, "## Skill: ", AGENT_CTX_MAX_CONTENT_LEN);
            }
            strlcat(system_prompt, item.provider_name, AGENT_CTX_MAX_CONTENT_LEN);
            strlcat(system_prompt, "\n", AGENT_CTX_MAX_CONTENT_LEN);
            strlcat(system_prompt, item.content, AGENT_CTX_MAX_CONTENT_LEN);
            break;

        case AGENT_CTX_KIND_TOOL_DEF:
            /* Tool definitions are passed separately via out_tools_json */
            if (out_tools_json && !*out_tools_json) {
                *out_tools_json = strdup(item.content);
            }
            break;
        }

        free(item.content);
    }

    xSemaphoreGive(pipeline->lock);

    /* Append current user turn */
    if (req->user_text && req->user_text[0]) {
        cJSON *user_msg = cJSON_CreateObject();
        if (user_msg) {
            cJSON_AddStringToObject(user_msg, "role", "user");
            cJSON_AddStringToObject(user_msg, "content", req->user_text);
            cJSON_AddItemToArray(messages, user_msg);
            total_tokens += agent_ctx_estimate_tokens(req->user_text);
        }
    }

    *out_messages_json = cJSON_PrintUnformatted(messages);
    cJSON_Delete(messages);

    if (!*out_messages_json) {
        free(system_prompt);
        return ESP_ERR_NO_MEM;
    }

    *out_system_prompt = system_prompt;

    if (out_token_estimate) {
        *out_token_estimate = total_tokens + agent_ctx_estimate_tokens(system_prompt);
    }

    ESP_LOGI(TAG, "Context built: providers=%u tokens=%u",
             (unsigned)pipeline->provider_count, (unsigned)total_tokens);
    return ESP_OK;
}

esp_err_t agent_ctx_pipeline_set_budget(agent_ctx_pipeline_t *pipeline,
                                         uint32_t token_budget)
{
    if (!pipeline) {
        return ESP_ERR_INVALID_ARG;
    }
    pipeline->token_budget = token_budget;
    return ESP_OK;
}

bool agent_ctx_pipeline_needs_compact(const agent_ctx_pipeline_t *pipeline,
                                       uint32_t threshold)
{
    if (!pipeline) {
        return false;
    }
    /* Simplified: always compact if threshold is set and budget is tight */
    return threshold > 0 && pipeline->token_budget > threshold;
}

esp_err_t agent_ctx_pipeline_compact(agent_ctx_pipeline_t *pipeline)
{
    if (!pipeline) {
        return ESP_ERR_INVALID_ARG;
    }
    /* Stub: in a full implementation, this would trim message history
     * by removing older messages or summarizing them.
     * For now, log that compact was requested.
     */
    ESP_LOGI(TAG, "Compact requested (stub — no action taken)");
    return ESP_OK;
}
