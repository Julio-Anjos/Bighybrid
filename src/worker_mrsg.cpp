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

#ifndef MRSG_WORKER_CODE
#define MRSG_WORKER_CODE

#include "common_bighybrid.hpp"
#include "dfs_mrsg.hpp"
#include "worker_mrsg.hpp"

XBT_LOG_EXTERNAL_DEFAULT_CATEGORY (msg_test);

static void mrsg_heartbeat (void);
/*OLD static int listen_mrsg (int argc, char* argv[]);*/
/*NEW*/ static void listen_mrsg();
/*OLD static int compute_mrsg (int argc, char* argv[]);*/
/*NEW*/ static void compute_mrsg (bighybrid_task_t mrsg_task);
static void update_mrsg_map_output (/*OLD msg_host_t*/ simgrid::s4u::Host* worker_mrsg, size_t mid);
static void get_mrsg_chunk (mrsg_task_info_t ti);
static void get_mrsg_map_output (mrsg_task_info_t ti);
void mrsg_kill_last_workers();

size_t get_mrsg_worker_id (/*OLD msg_host_t*/ simgrid::s4u::Host* worker_mrsg)
{
    /*NEW*/
    const char* wid = worker_mrsg->get_property("WID");
    size_t value = std::stoi(wid);
    return value;    
    /*NEW*/

    /*OLD
    w_mrsg_info_t  wi;

    wi = (w_mrsg_info_t) MSG_host_get_data (worker_mrsg);
    return wi->mrsg_wid;
    OLD*/
}
/**
 * @brief  Main worker function.
 *
 * This is the initial function of a worker node.
 * It creates other processes and runs a heartbeat loop.
 */
void worker_mrsg (int argc, char* argv[])
{

    char           mailbox[MAILBOX_ALIAS_SIZE];
    //OLD msg_host_t     me;
    /*NEW*/ simgrid::s4u::Host* me;

    //OLD me = MSG_host_self ();
    /*NEW*/ me = simgrid::s4u::Host::current();

    //OLD mrsg_task_pid.worker[get_mrsg_worker_id (me)+1] = MSG_process_self_PID();
    /*NEW*/ mrsg_task_pid.worker[get_mrsg_worker_id (me)+1] = simgrid::s4u::this_actor::get_pid();
    
    /* Spawn a process that listens for tasks. */
    //OLD MSG_process_create ("listen_mrsg", listen_mrsg, NULL, me);
    /*NEW*/ simgrid::s4u::Actor::create("listen_mrsg", me, listen_mrsg);
    
    /* Spawn a process to exchange data with other workers. */
    //OLD MSG_process_create ("data-node_mrsg", data_node_mrsg, NULL, me);
    /*NEW*/ simgrid::s4u::Actor::create("data-node_mrsg", me, data_node_mrsg);
    
    /* Start sending heartbeat signals to the master node. */
    mrsg_heartbeat ();

    sprintf (mailbox, DATANODE_MRSG_MAILBOX, get_mrsg_worker_id (me));
    send_mrsg_sms (SMS_FINISH_MRSG, mailbox);
    sprintf (mailbox, TASKTRACKER_MRSG_MAILBOX, get_mrsg_worker_id (me));
    send_mrsg_sms (SMS_FINISH_MRSG, mailbox);

    mrsg_kill_last_workers();

    mrsg_task_pid.workers_on--;
    mrsg_task_pid.worker[get_mrsg_worker_id (me)+1] = -1;
    mrsg_task_pid.status[get_mrsg_worker_id (me)+1] = OFF;
}

void mrsg_kill_last_workers()
{
  //OLD msg_process_t process_to_kill;
  /*NEW*/ simgrid::s4u::ActorPtr process_to_kill;
  for (int wid = 1; wid < config_mrsg.mrsg_number_of_workers+1; wid++) {
    //OLD if(mrsg_task_pid.status[wid]==ON && wid!=(get_mrsg_worker_id (MSG_host_self())+1))
    /*NEW*/ if(mrsg_task_pid.status[wid]==ON && (size_t) wid!=(get_mrsg_worker_id(simgrid::s4u::Host::current())+1))
    {
        //OLD process_to_kill = MSG_process_from_PID(mrsg_task_pid.worker[wid]);
        /*NEW*/ process_to_kill = simgrid::s4u::Actor::by_pid(mrsg_task_pid.worker[wid]);
        if(process_to_kill!=NULL)
            /*NEW*/ process_to_kill->kill();
            //OLD MSG_process_kill(process_to_kill);

        //OLD process_to_kill = MSG_process_from_PID(mrsg_task_pid.listen[wid]);
        /*NEW*/ process_to_kill = simgrid::s4u::Actor::by_pid(mrsg_task_pid.listen[wid]);
        if(process_to_kill!=NULL)
            /*NEW*/ process_to_kill->kill();
            //OLD MSG_process_kill(process_to_kill);

        //OLD process_to_kill = MSG_process_from_PID(mrsg_task_pid.data_node[wid]);
        /*NEW*/ process_to_kill = simgrid::s4u::Actor::by_pid(mrsg_task_pid.data_node[wid]);
        if(process_to_kill!=NULL)
            /*NEW*/ process_to_kill->kill();
            //OLD MSG_process_kill(process_to_kill);
    }
      }
}


/**
 * @brief  The heartbeat loop.
 */
static void mrsg_heartbeat (void)
{
    while (!job_mrsg.finished)
    {
		send_mrsg_sms (SMS_HEARTBEAT_MRSG, MASTER_MRSG_MAILBOX);
		//OLD MSG_process_sleep (config_mrsg.mrsg_heartbeat_interval);
        simgrid::s4u::this_actor::sleep_for(config_mrsg.mrsg_heartbeat_interval);
    }
}
/**
 * @brief  Process that listens for tasks.
 */
//OLD int
static void listen_mrsg (/*OLD int argc, char* argv[]*/)
{
    char         mailbox[MAILBOX_ALIAS_SIZE];
    /*OLD
    msg_error_t  status;
    msg_host_t   me;
    msg_task_t   msg = NULL;
    OLD*/
    /*NEW*/
    simgrid::s4u::Host* me;
    bighybrid_task_t msg;
    /*NEW*/

    //OLD me = MSG_host_self ();
    /*NEW*/ me = simgrid::s4u::Host::current();

    size_t wid =get_mrsg_worker_id (me)+1 ;
    //OLD mrsg_task_pid.listen[wid] = MSG_process_self_PID ();
    /*NEW*/ mrsg_task_pid.listen[wid] = simgrid::s4u::this_actor::get_pid();
    sprintf (mailbox, TASKTRACKER_MRSG_MAILBOX, get_mrsg_worker_id (me));

    while (!job_mrsg.finished)
    {
        /*OLD
	    msg = NULL;
	    status = receive (&msg, mailbox);
        OLD*/
        /*NEW*/  msg = receive(mailbox);

	    //OLD if (status == MSG_OK && mrsg_message_is (msg, SMS_TASK_MRSG))
	    /*NEW*/ if (msg && mrsg_message_is (msg, SMS_TASK_MRSG))
        {
	        //OLD MSG_process_create ("compute_mrsg", compute_mrsg, msg, me);
            /*NEW*/ simgrid::s4u::Actor::create("compute_mrsg", me, compute_mrsg, msg);
        }
	    else if (mrsg_message_is (msg, SMS_FINISH_MRSG))
	    {
	        //OLD MSG_task_destroy (msg);
	        /*NEW*/ msg->destroy();
            break;
	    }
    }

    //OLD return 0;
}
/**
 * @brief  Process that computes a task.
 */
//OLD int
static void compute_mrsg (/*int argc, char* argv[]*/ bighybrid_task_t mrsg_task)
{
    /*OLD
    msg_error_t  status;
    msg_task_t   mrsg_task;
    mrsg_task_info_t  ti;
  //  xbt_ex_t     e;

    mrsg_task = (msg_task_t) MSG_process_get_data (MSG_process_self ());
    ti = (mrsg_task_info_t) MSG_task_get_data (mrsg_task);
    ti->mrsg_pid = MSG_process_self_PID ();
    OLD*/
    mrsg_task_info_t  ti;
    /*NEW*/
    ti = (mrsg_task_info_t) mrsg_task->getData();
    ti->mrsg_pid = simgrid::s4u::this_actor::get_pid(); 
    /*NEW*/
    switch (ti->mrsg_phase)
    {
	case MRSG_MAP:
	    get_mrsg_chunk (ti);
	    break;

	case MRSG_REDUCE:
	    get_mrsg_map_output (ti);
	    break;
    }

    if (job_mrsg.task_status[ti->mrsg_phase][ti->mrsg_tid] != T_STATUS_MRSG_DONE)
    {
        //OLD status = MSG_task_execute (mrsg_task);
        /*NEW*/ mrsg_task->execute();

        if (ti->mrsg_phase == MRSG_MAP)
            /*NEW*/ update_mrsg_map_output (simgrid::s4u::Host::current(), ti->mrsg_tid);
            //OLD update_mrsg_map_output (MSG_host_self (), ti->mrsg_tid);
    /*
	TRY
	{
	    status = MSG_task_execute (mrsg_task);

	    if (ti->mrsg_phase == MRSG_MAP && status == MSG_OK)
		update_mrsg_map_output (MSG_host_self (), ti->mrsg_tid);
	}
	CATCH (e)
	{
	    xbt_assert (e.category == cancel_error, "%s", e.msg);
	    xbt_ex_free (e);
	}
    */
    }

    job_mrsg.mrsg_heartbeats[ti->mrsg_wid].slots_av[ti->mrsg_phase]++;

    if (!job_mrsg.finished)
	   send (SMS_TASK_MRSG_DONE, 0.0, 0.0, ti, MASTER_MRSG_MAILBOX);

    //OLD return 0;
}
/**
 * @brief  Update the amount of data produced by a mapper.
 * @param  worker_mrsg  The worker that finished a map task.
 * @param  mrsg_mid     The ID of map task.
 */
static void update_mrsg_map_output (/*msg_host_t*/ simgrid::s4u::Host* worker_mrsg, size_t mrsg_mid)
{
    int  mrsg_rid;
    size_t  mrsg_wid;

    mrsg_wid = get_mrsg_worker_id (worker_mrsg);

    for (mrsg_rid = 0; mrsg_rid < config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]; mrsg_rid++)
	job_mrsg.map_output[mrsg_wid][mrsg_rid] += user_mrsg.map_output_f (mrsg_mid, mrsg_rid);
}
/**
 * @brief  Get the chunk associated to a map task.
 * @param  ti  The task information.
 */
static void get_mrsg_chunk (mrsg_task_info_t ti)
{
    char         mailbox[MAILBOX_ALIAS_SIZE];
    /*OLD
    msg_error_t  status;
    msg_task_t   data = NULL;
    OLD*/
    size_t       my_id;
    /*NEW*/ bighybrid_task_t data;

    //OLD my_id = get_mrsg_worker_id (MSG_host_self ());
    /*NEW*/ my_id = get_mrsg_worker_id (simgrid::s4u::Host::current());

    /* Request the chunk to the source node. */
    if (ti->mrsg_src != my_id)
    {
	    sprintf (mailbox, DATANODE_MRSG_MAILBOX, ti->mrsg_src);
	    //OLD status = send_mrsg_sms (SMS_GET_MRSG_CHUNK, mailbox);
        send_mrsg_sms(SMS_GET_MRSG_CHUNK, mailbox);
    
        /*NEW*/
        sprintf (mailbox, TASK_MRSG_MAILBOX, my_id, simgrid::s4u::this_actor::get_pid());
        data = receive(mailbox);

        if(data)
            data->destroy();
        /*NEW*/
        /*OLD
        if (status == MSG_OK)
        {
            sprintf (mailbox, TASK_MRSG_MAILBOX, my_id, MSG_process_self_PID ());
            status = receive (&data, mailbox);
            if (status == MSG_OK)
            MSG_task_destroy (data);
        }
        OLD*/
    }
}
/**
 * @brief  Copy the itermediary pairs for a reduce task.
 * @param  ti  The task information.
 */
static void get_mrsg_map_output (mrsg_task_info_t ti)
{
    char         mailbox[MAILBOX_ALIAS_SIZE];
    /*OLD
    msg_error_t  status;
    msg_task_t   data = NULL;
    OLD*/
    size_t       total_copied, must_copy;
    size_t       my_id;
    int       mrsg_wid;
    size_t*      data_copied;
    /*NEW*/ bighybrid_task_t data;

    //OLD my_id = get_mrsg_worker_id (MSG_host_self ());
    /*NEW*/ my_id = get_mrsg_worker_id(simgrid::s4u::Host::current());
    data_copied = xbt_new0 (size_t, config_mrsg.mrsg_number_of_workers);
    ti->map_output_copied = data_copied;
    total_copied = 0;
    must_copy = reduce_mrsg_input_size (ti->mrsg_tid);

#ifdef VERBOSE
    XBT_INFO ("INFO: start copy");
#endif

    while (total_copied < must_copy)
    {
	for (mrsg_wid = 0; mrsg_wid < config_mrsg.mrsg_number_of_workers; mrsg_wid++)
	{
	    if (job_mrsg.task_status[MRSG_REDUCE][ti->mrsg_tid] == T_STATUS_MRSG_DONE)
	    {
		    xbt_free_ref (&data_copied);
		    return;
	    }

	    if (job_mrsg.map_output[mrsg_wid][ti->mrsg_tid] > data_copied[mrsg_wid])
	    {
		    sprintf (mailbox, DATANODE_MRSG_MAILBOX, (size_t)mrsg_wid);
            send(SMS_GET_INTER_MRSG_PAIRS, 0.0, 0.0, ti, mailbox);
            /*OLD 
            status = send (SMS_GET_INTER_MRSG_PAIRS, 0.0, 0.0, ti, mailbox);
            if (status == MSG_OK)
            {
                sprintf (mailbox, TASK_MRSG_MAILBOX, my_id, MSG_process_self_PID ());
            }
            OLD*/
            sprintf (mailbox, TASK_MRSG_MAILBOX, my_id, simgrid::s4u::this_actor::get_pid());
		    //TODO Set a timeout: reduce.copy.backoff
		    /*NEW*/ data = receive (mailbox);
		    
            if (data)
		    {
                /*NEW*/
                data_copied[mrsg_wid] += data->getBytesAmount();
                total_copied += data->getBytesAmount();   
                data->destroy();
			    /*NEW*/
                /*OLD
                data_copied[mrsg_wid] += MSG_task_get_bytes_amount (data);
			    total_copied += MSG_task_get_bytes_amount (data);
			    MSG_task_destroy (data);
                OLD
                */
            }
		}
	}
	/* (Hadoop 0.20.2) mapred/ReduceTask.java:1979 */
	//OLD MSG_process_sleep (5);
    /*NEW*/ simgrid::s4u::this_actor::sleep_for(5);
    }
#ifdef VERBOSE
    XBT_INFO ("INFO: copy finished");
#endif
    //OLD ti->shuffle_mrsg_end = MSG_get_clock ();
    /*NEW*/ ti->shuffle_mrsg_end = simgrid::s4u::Engine::get_clock();

    xbt_free_ref (&data_copied);
}

#endif