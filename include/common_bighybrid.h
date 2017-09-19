/* Copyright (c) 2014. BigHybrid Team. All rights reserved. */

/* This file is part of BigHybrid.

BigHybrid, MRSG and MRA++ are free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

BigHybrid, MRSG and MRA++ are distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with BigHybrid, MRSG and MRA++.  If not, see <http://www.gnu.org/licenses/>. */

#ifndef BIGHYBRID_COMMON_H
#define BIGHYBRID_COMMON_H

#include <simgrid/msg.h>
#include <xbt/sysdep.h>
#include <xbt/log.h>
#include <xbt/asserts.h>
#include "bighybrid.h"


/** @brief  Initialize dist_bruta, task_exec, avg_task_exec. */

int*        dist_bruta;
double*     avg_task_exec_map;
double*     avg_task_exec_reduce;




/* Hearbeat parameters. */
#define MRA_HEARTBEAT_MIN_INTERVAL 3
#define MRSG_HEARTBEAT_MIN_INTERVAL 3

#define MRA_HEARTBEAT_TIMEOUT 600
#define MRSG_HEARTBEAT_TIMEOUT 600

#define ON 1
#define OFF -1
/* Short message names. */
#define SMS_GET_MRA_CHUNK "SMS-MRA-GC"
#define SMS_GET_MRSG_CHUNK "SMS-MRSG-GC"

#define SMS_GET_INTER_MRA_PAIRS "SMS-MRA-GIP"
#define SMS_GET_INTER_MRSG_PAIRS "SMS-MRSG-GIP"

#define SMS_HEARTBEAT_MRA "SMS-MRA-HB"
#define SMS_HEARTBEAT_MRSG "SMS-MRSG-HB"

#define SMS_TASK_MRA "SMS-MRA-T"
#define SMS_TASK_MRSG "SMS-MRSG-T"

#define SMS_TASK_MRA_DONE "SMS-MRA-TD"
#define SMS_TASK_MRSG_DONE "SMS-MRSG-TD"

#define SMS_FINISH_MRA "SMS-MRA-F"
#define SMS_FINISH_MRSG "SMS-MRSG-F"

#define NONE (-1)
#define MAX_SPECULATIVE_COPIES 3

/* Mailbox related. */
#define MAILBOX_ALIAS_SIZE 256

#define MASTER_MRA_MAILBOX "MASTER_MRA"
#define MASTER_MRSG_MAILBOX "MASTER_MRSG"

#define DATANODE_MRA_MAILBOX "%zu:MRADN"
#define DATANODE_MRSG_MAILBOX "%zu:MRSGDN"

#define TASKTRACKER_MRA_MAILBOX "%zu:MRATT"
#define TASKTRACKER_MRSG_MAILBOX "%zu:MRSGTT"

#define TASK_MRA_MAILBOX "%zu:%d"
#define TASK_MRSG_MAILBOX "%zu:%d"

/** @brief  Possible MRA task status. */
enum mra_task_status_e {
    /* The initial status must be the first enum. */
    T_STATUS_MRA_PENDING,
    T_STATUS_MRA_TIP,
    T_STATUS_MRA_TIP_SLOW,
    T_STATUS_MRA_DONE,
    T_STATUS_MRA_DISP,
    T_STATUS_MRA_FAILURE
};

/** @brief  Possible MRSG task status. */
enum mrsg_task_status_e {
    /* The initial status must be the first enum. */
    T_STATUS_MRSG_PENDING,
    T_STATUS_MRSG_TIP,
    T_STATUS_MRSG_TIP_SLOW,
    T_STATUS_MRSG_DONE
};

/** @brief  Information about dist_bruta. */
struct mra_dist_mang_s {
       int min_tot_dist;
       int max_tot_dist;
       int*  total_dist;
} mra_dist_manage;

/** @brief  Information sent by the workers with every mra_heartbeat. */
struct mra_heartbeat_s {
    int  slots_av[2];
    long double wid_timestamp;
};

typedef struct mra_heartbeat_s* mra_heartbeat_t;

struct mra_config_s {
    double         mra_chunk_size;
    double         grid_average_speed;
    double         grid_cpu_power;
    int            mra_chunk_count;
    int            mra_chunk_replicas;
    int            mra_heartbeat_interval;
    int            amount_of_tasks_mra[2];
    int            mra_number_of_workers;
    int            mra_slots[2];
    double         mra_perc;
    double         cpu_required_reduce_mra;
    double         cpu_required_map_mra;
    double 				 mra_map_task_cost;
    double         mra_reduce_task_cost;
    int            Fg;
    double	       perc_vc_node;
    double         failure_timeout_conf;
    int            initialized;
    double 			   mra_bandwidth;
    msg_host_t*    workers_mra;
} config_mra;

struct mra_job_s {
    int           	finished;
    int           	tasks_pending[2];
    int*          	task_instances[2];
    int*          	task_status[2];
    msg_task_t**  	task_list[2];
    size_t**      	map_output;
    mra_heartbeat_t	mra_heartbeats;
    long double   	wid_timestamp;
    int**  				mra_task_dist[2];
} job_mra;

/** @brief  Information sent as the mra_task data. */
struct mra_task_info_s {
    enum mra_phase_e  mra_phase;
    size_t        mra_tid;
    size_t        mra_src;
    size_t        mra_wid;
    int           mra_pid;
    msg_task_t    mra_task;
    size_t*       map_output_copied;
    double        shuffle_mra_end;
};

typedef struct mra_task_info_s* mra_task_info_t;

struct mra_stats_s {
    enum mra_phase_e  mra_phase;
    int   map_local_mra;
    int   mra_map_remote;
    int   map_spec_mra_l;
    int   map_spec_mra_r;
    int   mra_map_recovery;
    int	  mra_reduce_recovery;
    int   reduce_mra_normal;
    int   reduce_mra_spec;
    double map_time;
    double reduce_time;
} stats_mra;

struct mra_user_s {
    double (*task_mra_cost_f)(enum mra_phase_e mra_phase, size_t tid_mra, size_t mra_wid);
    void (*mra_dfs_f)(char** mra_dfs_matrix, size_t chunks, size_t workers_mra, int replicas);
    int (*map_mra_output_f)(size_t mra_mid, size_t mra_rid);
} user_mra;



/** @brief  Information sent by the workers with every mrsg_heartbeat. */
struct mrsg_heartbeat_s {
    int  slots_av[2];
};

typedef struct mrsg_heartbeat_s* mrsg_heartbeat_t;

struct mrsg_config_s {
    double         mrsg_chunk_size;
    double         grid_average_speed;
    double         grid_cpu_power;
    int            mrsg_chunk_count;
    int            mrsg_chunk_replicas;
    int            mrsg_heartbeat_interval;
    int            amount_of_tasks_mrsg[2];
    int            mrsg_number_of_workers;
    int            mrsg_slots[2];
    double         mrsg_perc;
    double         cpu_required_reduce_mrsg;
    double         cpu_required_map_mrsg;
    double 				 mrsg_map_task_cost;
    double         mrsg_reduce_task_cost;
    int            initialized;
    msg_host_t*    workers_mrsg;
} config_mrsg;

typedef struct {
  int * listen;
  int * data_node;
  int * worker;
  int workers_on;
  int * status;
}task_pid;

task_pid mrsg_task_pid;
task_pid mra_task_pid;

struct mrsg_job_s {
    int           finished;
    int           tasks_pending[2];
    int*          task_instances[2];
    int*          task_status[2];
    msg_task_t**  task_list[2];
    size_t**      map_output;
    mrsg_heartbeat_t   mrsg_heartbeats;
} job_mrsg;

/** @brief  Information sent as the mrsg_task data. */
struct mrsg_task_info_s {
    enum mrsg_phase_e  mrsg_phase;
    size_t        mrsg_tid;
    size_t        mrsg_src;
    size_t        mrsg_wid;
    int           mrsg_pid;
    msg_task_t    mrsg_task;
    size_t*       map_output_copied;
    double        shuffle_mrsg_end;
};

typedef struct mrsg_task_info_s* mrsg_task_info_t;

struct mrsg_stats_s {
    int   map_local_mrsg;
    int   map_remote_mrsg;
    int   map_spec_mrsg_l;
    int   map_spec_mrsg_r;
    int   reduce_mrsg_normal;
    int   reduce_mrsg_spec;
    double map_time;
    double reduce_time;
} stats_mrsg;

struct mrsg_user_s {
    double (*task_cost_f)(enum mrsg_phase_e mrsg_phase, size_t tid_mrsg, size_t mrsg_wid);
    void (*dfs_f)(char** dfs_matrix, size_t chunks, size_t workers_mrsg, int replicas);
    int (*map_output_f)(size_t mrsg_mid, size_t mrsg_rid);
} user_mrsg;


/**
 * @brief  Send a message/task.
 * @param  str      The message.
 * @param  cpu      The amount of cpu required by the task.
 * @param  net      The message size in bytes.
 * @param  data     Any data to attatch to the message.
 * @param  mailbox  The destination mailbox alias.
 * @return The MSG status of the operation.
 */
msg_error_t mra_send (const char* str, double cpu, double net, void* data, const char* mailbox);

/**
 * @brief  Send a message/task.
 * @param  str      The message.
 * @param  cpu      The amount of cpu required by the task.
 * @param  net      The message size in bytes.
 * @param  data     Any data to attatch to the message.
 * @param  mailbox  The destination mailbox alias.
 * @return The MSG status of the operation.
 */
msg_error_t send (const char* str, double cpu, double net, void* data, const char* mailbox);

/**
 * @brief  Send a short message, of size zero.
 * @param  str      The message.
 * @param  mailbox  The destination mailbox alias.
 * @return The MSG MRA_status of the operation.
 */
msg_error_t send_mra_sms (const char* str, const char* mailbox);

/**
 * @brief  Send a short message, of size zero.
 * @param  str      The message.
 * @param  mailbox  The destination mailbox alias.
 * @return The MSG MRSG_status of the operation.
 */
msg_error_t send_mrsg_sms (const char* str, const char* mailbox);

/**
 * @brief  Receive a message/task from a mailbox.
 * @param  msg      Where to store the received message.
 * @param  mailbox  The mailbox alias.
 * @return The status of the transfer.
 */
msg_error_t mra_receive (msg_task_t* msg_mra, const char* mailbox);


/**
 * @brief  Receive a message/task from a mailbox.
 * @param  msg      Where to store the received message.
 * @param  mailbox  The mailbox alias.
 * @return The status of the transfer.
 */
msg_error_t receive (msg_task_t* msg, const char* mailbox);


/**
 * @brief  Compare the message from a task with a string.
 * @param  msg  The message/task.
 * @param  str  The string to compare with.
 * @return A positive value if matches, zero if doesn't.
 */
int mra_message_is (msg_task_t msg_mra, const char* str);
int mrsg_message_is (msg_task_t msg, const char* str);

/**
 * @brief  Return the maximum of two values.
 */
int mra_maxval (int mra_a, int mra_b);
int mrsg_maxval (int mrsg_a, int mrsg_b);

/**
 * @brief  Return the Map output size.
 */
size_t map_mra_output_size (size_t mra_mid);
size_t map_mrsg_output_size (size_t mrsg_mid);

/**
 * @brief  Return the Reduce input size.
 */

size_t reduce_mra_input_size (size_t mra_rid);
size_t reduce_mrsg_input_size (size_t mrsg_rid);


/**
 * @brief  Return the bandwidth.
 */
void read_bandwidth_mra (const char* plat);

#endif /* !BIGHYBRID_COMMON_H */
