/*
 * SPDX-FileCopyrightText: 2026 Espressif Systems (Shanghai) CO LTD
 *
 * SPDX-License-Identifier: Apache-2.0
 */
#include "claw_agent_state_machine.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "esp_log.h"

static const char *TAG = "agent_sm";

#define AGENT_SM_MAX_TRANSITIONS 32

struct agent_state_machine {
    agent_state_t current;
    agent_state_transition_t transitions[AGENT_SM_MAX_TRANSITIONS];
    size_t transition_count;
};

/* Default transition table — Claude Code inspired query loop */
static const agent_state_transition_t s_default_transitions[] = {
    /* Idle → start building context */
    { AGENT_STATE_IDLE,          AGENT_EVENT_START,              AGENT_STATE_CONTEXT_BUILD },

    /* Context ready → call LLM */
    { AGENT_STATE_CONTEXT_BUILD, AGENT_EVENT_CONTEXT_READY,      AGENT_STATE_LLM_CALL },

    /* LLM streaming → first chunk arrived */
    { AGENT_STATE_LLM_CALL,      AGENT_EVENT_LLM_FIRST_CHUNK,    AGENT_STATE_STREAMING },

    /* Streaming → tool call detected */
    { AGENT_STATE_STREAMING,     AGENT_EVENT_LLM_TOOL_CALL,      AGENT_STATE_TOOL_DISPATCH },

    /* Streaming → text done, no tools */
    { AGENT_STATE_STREAMING,     AGENT_EVENT_LLM_DONE,           AGENT_STATE_RESPONDING },

    /* Tool dispatch → execute */
    { AGENT_STATE_TOOL_DISPATCH, AGENT_EVENT_TOOL_BATCH_START,   AGENT_STATE_TOOL_EXECUTE },

    /* Tool execution → observe results */
    { AGENT_STATE_TOOL_EXECUTE,  AGENT_EVENT_TOOL_BATCH_DONE,    AGENT_STATE_OBSERVE },

    /* Observe → continue (loop back to context build) */
    { AGENT_STATE_OBSERVE,       AGENT_EVENT_CONTINUE,           AGENT_STATE_CONTEXT_BUILD },

    /* Observe → max turns reached */
    { AGENT_STATE_OBSERVE,       AGENT_EVENT_MAX_TURNS,          AGENT_STATE_DONE },

    /* Compact required */
    { AGENT_STATE_CONTEXT_BUILD, AGENT_EVENT_COMPACT_REQUIRED,   AGENT_STATE_COMPACT },
    { AGENT_STATE_COMPACT,       AGENT_EVENT_CONTEXT_READY,      AGENT_STATE_LLM_CALL },

    /* Cancellation */
    { AGENT_STATE_IDLE,          AGENT_EVENT_CANCEL,             AGENT_STATE_CANCELLED },
    { AGENT_STATE_CONTEXT_BUILD, AGENT_EVENT_CANCEL,             AGENT_STATE_CANCELLED },
    { AGENT_STATE_LLM_CALL,      AGENT_EVENT_CANCEL,             AGENT_STATE_CANCELLED },
    { AGENT_STATE_STREAMING,     AGENT_EVENT_CANCEL,             AGENT_STATE_CANCELLED },
    { AGENT_STATE_TOOL_DISPATCH, AGENT_EVENT_CANCEL,             AGENT_STATE_CANCELLED },
    { AGENT_STATE_TOOL_EXECUTE,  AGENT_EVENT_CANCEL,             AGENT_STATE_CANCELLED },
    { AGENT_STATE_OBSERVE,       AGENT_EVENT_CANCEL,             AGENT_STATE_CANCELLED },

    /* Error recovery */
    { AGENT_STATE_LLM_CALL,      AGENT_EVENT_ERROR,              AGENT_STATE_ERROR },
    { AGENT_STATE_STREAMING,     AGENT_EVENT_ERROR,              AGENT_STATE_ERROR },
    { AGENT_STATE_TOOL_EXECUTE,  AGENT_EVENT_ERROR,              AGENT_STATE_ERROR },

    /* Error → done (terminal) */
    { AGENT_STATE_ERROR,         AGENT_EVENT_LLM_DONE,           AGENT_STATE_DONE },
};

agent_state_machine_t *agent_state_machine_create(void)
{
    agent_state_machine_t *sm = calloc(1, sizeof(*sm));
    if (!sm) {
        return NULL;
    }
    sm->current = AGENT_STATE_IDLE;
    sm->transition_count = 0;

    /* Load default transitions */
    size_t default_count = sizeof(s_default_transitions) / sizeof(s_default_transitions[0]);
    if (default_count > AGENT_SM_MAX_TRANSITIONS) {
        default_count = AGENT_SM_MAX_TRANSITIONS;
    }
    memcpy(sm->transitions, s_default_transitions,
           default_count * sizeof(agent_state_transition_t));
    sm->transition_count = default_count;

    return sm;
}

void agent_state_machine_destroy(agent_state_machine_t *sm)
{
    free(sm);
}

esp_err_t agent_state_machine_register_transition(agent_state_machine_t *sm,
                                                   const agent_state_transition_t *trans,
                                                   size_t count)
{
    if (!sm || !trans) {
        return ESP_ERR_INVALID_ARG;
    }
    if (sm->transition_count + count > AGENT_SM_MAX_TRANSITIONS) {
        return ESP_ERR_NO_MEM;
    }
    for (size_t i = 0; i < count; i++) {
        sm->transitions[sm->transition_count++] = trans[i];
    }
    return ESP_OK;
}

static const agent_state_transition_t *find_transition(const agent_state_machine_t *sm,
                                                        agent_state_t from,
                                                        agent_event_t event)
{
    for (size_t i = 0; i < sm->transition_count; i++) {
        if (sm->transitions[i].from == from && sm->transitions[i].event == event) {
            return &sm->transitions[i];
        }
    }
    return NULL;
}

esp_err_t agent_state_machine_dispatch(agent_state_machine_t *sm,
                                        agent_event_t event,
                                        agent_state_t *out_new_state)
{
    if (!sm) {
        return ESP_ERR_INVALID_ARG;
    }

    const agent_state_transition_t *trans = find_transition(sm, sm->current, event);
    if (!trans) {
        ESP_LOGW(TAG, "No transition from %s on event %s",
                 agent_state_to_string(sm->current),
                 agent_event_to_string(event));
        return ESP_ERR_INVALID_STATE;
    }

    ESP_LOGD(TAG, "Transition: %s --[%s]--> %s",
             agent_state_to_string(sm->current),
             agent_event_to_string(event),
             agent_state_to_string(trans->to));

    sm->current = trans->to;
    if (out_new_state) {
        *out_new_state = sm->current;
    }
    return ESP_OK;
}

agent_state_t agent_state_machine_get_state(const agent_state_machine_t *sm)
{
    return sm ? sm->current : AGENT_STATE_IDLE;
}

bool agent_state_machine_can_handle(const agent_state_machine_t *sm,
                                     agent_state_t from,
                                     agent_event_t event)
{
    if (!sm) {
        return false;
    }
    return find_transition(sm, from, event) != NULL;
}

const char *agent_state_to_string(agent_state_t state)
{
    switch (state) {
    case AGENT_STATE_IDLE:           return "IDLE";
    case AGENT_STATE_CONTEXT_BUILD:  return "CONTEXT_BUILD";
    case AGENT_STATE_LLM_CALL:       return "LLM_CALL";
    case AGENT_STATE_STREAMING:      return "STREAMING";
    case AGENT_STATE_TOOL_DISPATCH:  return "TOOL_DISPATCH";
    case AGENT_STATE_TOOL_EXECUTE:   return "TOOL_EXECUTE";
    case AGENT_STATE_OBSERVE:        return "OBSERVE";
    case AGENT_STATE_RESPONDING:     return "RESPONDING";
    case AGENT_STATE_COMPACT:        return "COMPACT";
    case AGENT_STATE_DONE:           return "DONE";
    case AGENT_STATE_ERROR:          return "ERROR";
    case AGENT_STATE_CANCELLED:      return "CANCELLED";
    default:                         return "UNKNOWN";
    }
}

const char *agent_event_to_string(agent_event_t event)
{
    switch (event) {
    case AGENT_EVENT_START:              return "START";
    case AGENT_EVENT_CONTEXT_READY:      return "CONTEXT_READY";
    case AGENT_EVENT_LLM_FIRST_CHUNK:    return "LLM_FIRST_CHUNK";
    case AGENT_EVENT_LLM_TOOL_CALL:      return "LLM_TOOL_CALL";
    case AGENT_EVENT_LLM_TEXT_DELTA:     return "LLM_TEXT_DELTA";
    case AGENT_EVENT_LLM_DONE:           return "LLM_DONE";
    case AGENT_EVENT_TOOL_BATCH_START:   return "TOOL_BATCH_START";
    case AGENT_EVENT_TOOL_BATCH_DONE:    return "TOOL_BATCH_DONE";
    case AGENT_EVENT_TOOL_RESULT:        return "TOOL_RESULT";
    case AGENT_EVENT_CONTINUE:           return "CONTINUE";
    case AGENT_EVENT_MAX_TURNS:          return "MAX_TURNS";
    case AGENT_EVENT_COMPACT_REQUIRED:   return "COMPACT_REQUIRED";
    case AGENT_EVENT_CANCEL:             return "CANCEL";
    case AGENT_EVENT_ERROR:              return "ERROR";
    default:                             return "UNKNOWN";
    }
}
