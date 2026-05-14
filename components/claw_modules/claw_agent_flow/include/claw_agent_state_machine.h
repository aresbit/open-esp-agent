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

/* State transition table entry */
typedef struct {
    agent_state_t from;
    agent_event_t event;
    agent_state_t to;
} agent_state_transition_t;

/* State machine handle (opaque) */
typedef struct agent_state_machine agent_state_machine_t;

/* Create/destroy */
agent_state_machine_t *agent_state_machine_create(void);
void agent_state_machine_destroy(agent_state_machine_t *sm);

/* Configuration */
esp_err_t agent_state_machine_register_transition(agent_state_machine_t *sm,
                                                   const agent_state_transition_t *trans,
                                                   size_t count);

/* Event handling */
esp_err_t agent_state_machine_dispatch(agent_state_machine_t *sm,
                                        agent_event_t event,
                                        agent_state_t *out_new_state);

/* Queries */
agent_state_t agent_state_machine_get_state(const agent_state_machine_t *sm);
bool agent_state_machine_can_handle(const agent_state_machine_t *sm,
                                     agent_state_t from,
                                     agent_event_t event);
const char *agent_state_to_string(agent_state_t state);
const char *agent_event_to_string(agent_event_t event);

#ifdef __cplusplus
}
#endif
