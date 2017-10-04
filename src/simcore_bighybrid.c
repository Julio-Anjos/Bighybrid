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

#include <stdio.h>
#include <stdlib.h>
#include <simgrid/msg.h>
#include <xbt/sysdep.h>
#include <xbt/log.h>
#include <xbt/asserts.h>
#include <string.h>

#include "common_bighybrid.h"
#include "worker_mra.h"
#include "worker_mrsg.h"
#include "dfs_mra.h"
#include "dfs_mrsg.h"
#include "bighybrid.h"
#include "mra_cv.h"


XBT_LOG_NEW_DEFAULT_CATEGORY (msg_test, "BIGHYBRID");

#define MAX_LINE_SIZE 1024

// MRA
int master_mra (int argc, char *argv[]);
int worker_mra (int argc, char *argv[]);
static void check_config_mra (void);
static void init_mr_mra_config (const char* bighybrid_config_file);
static void init_mra_config (void);
static void init_job_mra (void);
static void init_mra_stats (void);
static void free_mra_global_mem (void);
char res_mra;

//Initialize VC
static void init_mra_vc (const char* vc_file_name);
static int mra_vc_prep_traces (const char* vc_file_name);
static int read_mra_vc_config_file (const char* vc_file_name, int n_line);
//End VC

//MRSG

int master_mrsg (int argc, char *argv[]);
int worker_mrsg (int argc, char *argv[]);
static void check_config_mrsg (void);
static void init_mr_mrsg_config (const char* bighybrid_config_file);
static void init_mrsg_config (void);
static void init_job_mrsg (void);
static void init_mrsg_stats (void);
static void free_mrsg_global_mem (void);
char res_mrsg;

// BigHybrid

static msg_error_t run_hybrid_simulation (const char* platform_file, const char* deploy_file, const char* bighybrid_config_file, const char* vc_file);
static void read_bighybrid_config_file (const char* file_name);
void print_bighybrid_output();

/*
 * @brief Initialize BigHybrid main.
 * @param  plat  	The path/name of the MRA platform file.
 * @param  depl  	The path/name of the MRA deploy file.
 * @param  conf  	The path/name of the MRA configuration file.
 */


int BIGHYBRID_main (const char* plat, const char* depl, const char* conf, const char* vc_file)
{
 		msg_error_t  res_bighybrid = MSG_OK;


    config_mra.initialized = 0;
    config_mrsg.initialized = 0;

    check_config_mra ();
    check_config_mrsg ();



    res_bighybrid = run_hybrid_simulation (plat, depl, conf, vc_file);

     print_bighybrid_output();


    if (res_bighybrid == MSG_OK && job_mrsg.finished == 1 && job_mra.finished == 1)
     {
   		return 0;
     }
    else{
 	  	return 1;
      }

}

/**
 * @brief Check if the user MRA configuration is sound.
 */
static void check_config_mra (void)
{
    xbt_assert (user_mra.task_mra_cost_f != NULL, "Task cost function not specified.");
    xbt_assert (user_mra.map_mra_output_f != NULL, "Map output function not specified.");
}

/**
 * @brief Check if the user MRSG configuration is sound.
 */
static void check_config_mrsg (void)
{
    xbt_assert (user_mrsg.task_cost_f != NULL, "Task cost function not specified.");
    xbt_assert (user_mrsg.map_output_f != NULL, "Map output function not specified.");
}

/*
 * @brief Initialize MRSG  msg_error.
 * @param  platform_file   The path/name of the platform file.
 * @param  deploy_file     The path/name of the deploy file.
 * @param  bighybrid_config_file  The path/name of the configuration file.
 */
static msg_error_t run_hybrid_simulation (const char* platform_file, const char* deploy_file, const char* bighybrid_config_file, const char* vc_file_name)
{
    msg_error_t  res_bighybrid = MSG_OK;


    read_bighybrid_config_file (bighybrid_config_file);


		init_mra_vc (vc_file_name);

		read_bandwidth_mra (platform_file);


    MSG_create_environment (platform_file);

    // for tracing purposes..
   TRACE_category_with_color ("MRA_MAP", "1 0 0");
   TRACE_category_with_color ("MRA_REDUCE", "0 0 1");
   TRACE_category_with_color ("MRSG_MAP", "1 0 0");
   TRACE_category_with_color ("MRSG_REDUCE", "0 0 1");
   TRACE_category ("finalize");

    MSG_function_register ("master_mra", master_mra);
    MSG_function_register ("master_mrsg", master_mrsg);
    MSG_function_register ("worker_mra", worker_mra);
    MSG_function_register ("worker_mrsg", worker_mrsg);


    MSG_launch_application (deploy_file);

    init_mr_mrsg_config (bighybrid_config_file);
    init_mr_mra_config (bighybrid_config_file);

    res_bighybrid = MSG_main ();


		free_mra_global_mem ();
    free_mrsg_global_mem ();

    return res_bighybrid;
}



/**
 * @brief  Initialize the MapReduce configuration in MRA.
 * @param  bighybrid_config_file  The path/name of the configuration file.
 */
static void init_mr_mra_config (const char* bighybrid_config_file)
{
    srand (12345);
    init_mra_config ();
    init_mra_stats ();
    init_job_mra ();
    distribute_data_mra ();
}

/**
 * @brief Initialize the Volunteer Computing Traces.
 * @param vc_file_name
 *
 */

static void init_mra_vc (const char* vc_file_name){

    int n_line;
    n_line = mra_vc_prep_traces (vc_file_name);
    read_mra_vc_config_file (vc_file_name, n_line);
}

/*
 *
 * @brief  Initialize the MapReduce configuration in MRSG.
 * @param  bighybrid_config_file  The path/name of the configuration file.
 */
static void init_mr_mrsg_config (const char* bighybrid_config_file)
{
    srand (12345);
    init_mrsg_config ();
    init_mrsg_stats ();
    init_job_mrsg ();
    distribute_data_mrsg ();
}


/*
* @brief Reads config file and creates a vector to each node with the availability type, and start and end times.
*/
static int read_mra_vc_config_file (const char* vc_file_name, int n_line)
{
    int   i;
    /* Allocate memory for the volunteer computing matrix. */
   	vc_node = xbt_new(int*, (n_line * sizeof (int)));
   	vc_type = xbt_new(int*, (n_line * sizeof (int)));
   	vc_start = xbt_new(long double*, (n_line * sizeof (long double)));
   	vc_end = xbt_new(long double*, (n_line * sizeof (long double)));
   	/* Allocate memory for the elements of volunteer computing matrix. */
    for (i = 0; i < (n_line + 1); i++)
    {
     		vc_node[i] = xbt_new(int, (sizeof (int)));
     		vc_type[i] = xbt_new(int, (sizeof (int)));
     		vc_start[i] = xbt_new(long double, (sizeof (long double)));
     		vc_end[i] = xbt_new(long double, (sizeof (long double)));
    }

    VC_TRACE *ptr_one;
    FILE* vc_file;

    ptr_one = (VC_TRACE *) malloc(n_line * sizeof(VC_TRACE));
    vc_file = fopen (vc_file_name, "r");

    i=0;
    while(!feof(vc_file))
    {
        fscanf(vc_file, "%d,%d,%Lf,%Lf", &ptr_one->mra_vc_node_id, &ptr_one->mra_vc_status_type,
                                  &ptr_one->mra_vc_start, &ptr_one->mra_vc_end);

        vc_node[i][0] = ptr_one->mra_vc_node_id;
        vc_type[i][0] = ptr_one->mra_vc_status_type;
        vc_start[i][0] = ptr_one->mra_vc_start;
        vc_end[i][0]= ptr_one->mra_vc_end;
    		i++;
    }

    return 0;

    fclose(vc_file);
}

/* @brief Return the number of lines on vc_file
*  @param vc_file_name
*  @return n_line
*/
static int mra_vc_prep_traces (const char* vc_file_name)
{

    int n_line = 0;
    char c;
    FILE* file;
    /* Read the user configuration file. */
    file = fopen (vc_file_name, "r");

    // Extract characters from file and store in character n_line
    for (c = getc(file); c != EOF; c = getc(file)){
        if (c == '\n')
            n_line++;
  			}
        fclose(file);
   /*Array element with number lines of vc_fine_name. The result is saved on element array config_mra_vc_file_line[0] */
    config_mra_vc_file_line[0] = n_line;
    return n_line;
}

/**
 * @brief  Read the MapReduce BigHybrid configuration file.
 * @param  file_name  The path/name of the configuration file.
 */
static void read_bighybrid_config_file (const char* file_name)
{
    char    property[256];
    FILE*   file;

    /* Set the default MRA configuration. */
    config_mra.mra_chunk_size = 67108864;
    config_mra.mra_chunk_count = 0;
    config_mra.mra_chunk_replicas = 3;
    config_mra.mra_slots[MRA_MAP] = 2;
    config_mra.amount_of_tasks_mra[MRA_REDUCE] = 1;
    config_mra.mra_slots[MRA_REDUCE] = 2;
    config_mra.Fg=1;
    config_mra.mra_perc=100;

   /* Set the default MRSG configuration. */
    config_mrsg.mrsg_chunk_size = 67108864;
    config_mrsg.mrsg_chunk_count = 0;
    config_mrsg.mrsg_chunk_replicas = 3;
    config_mrsg.mrsg_slots[MRSG_MAP] = 2;
    config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE] = 1;
    config_mrsg.mrsg_slots[MRSG_REDUCE] = 2;
    config_mrsg.mrsg_perc = 100;

    /* Read the user configuration file. */

    file = fopen (file_name, "r");

    xbt_assert (file != NULL, "Error reading cofiguration file: %s", file_name);

    while ( fscanf (file, "%256s", property) != EOF )
    {
	if ( strcmp (property, "mra_chunk_size") == 0 )
	{
	    fscanf (file, "%lg", &config_mra.mra_chunk_size);
	    config_mra.mra_chunk_size *= 1024 * 1024; /* MB -> bytes */
	}
	else if ( strcmp (property, "mra_input_chunks") == 0 )
	{
	    fscanf (file, "%d", &config_mra.mra_chunk_count);
	}
	else if ( strcmp (property, "mra_dfs_replicas") == 0 )
	{
	    fscanf (file, "%d", &config_mra.mra_chunk_replicas);
	}
	else if ( strcmp (property, "mra_map_slots") == 0 )
	{
	    fscanf (file, "%d", &config_mra.mra_slots[MRA_MAP]);
	}
	else if ( strcmp (property, "grain_factor") == 0 )
	{
	    fscanf (file, "%d", &config_mra.Fg);
	}
	else if ( strcmp (property, "mra_intermed_perc") == 0 )
	{
	    fscanf (file, "%lg", &config_mra.mra_perc);
	}
	else if ( strcmp (property, "mra_reduces") == 0 )
	{
	    fscanf (file, "%d", &config_mra.amount_of_tasks_mra[MRA_REDUCE]);
	}
	else if ( strcmp (property, "mra_reduce_slots") == 0 )
	{
	    fscanf (file, "%d", &config_mra.mra_slots[MRA_REDUCE]);
	}
	else if ( strcmp (property, "mra_map_task_cost") == 0 )
	{
	    fscanf (file, "%lg", &config_mra.mra_map_task_cost);
	}
	else if ( strcmp (property, "mra_reduce_task_cost") == 0 )
	{
	    fscanf (file, "%lg", &config_mra.mra_reduce_task_cost);
	}
	else if ( strcmp (property, "perc_num_volatile_node") == 0 )
	{
	    fscanf (file, "%lg", &config_mra.perc_vc_node);
	}
	else if ( strcmp (property, "failure_timeout") == 0 )
	{
	    fscanf (file, "%lg", &config_mra.failure_timeout_conf);
	}
	else if ( strcmp (property, "mrsg_chunk_size") == 0 )
	{
	    fscanf (file, "%lg", &config_mrsg.mrsg_chunk_size);
	    config_mrsg.mrsg_chunk_size *= 1024 * 1024; /* MB -> bytes */
	}
	else if ( strcmp (property, "mrsg_input_chunks") == 0 )
	{
	    fscanf (file, "%d", &config_mrsg.mrsg_chunk_count);
	}
	else if ( strcmp (property, "mrsg_dfs_replicas") == 0 )
	{
	    fscanf (file, "%d", &config_mrsg.mrsg_chunk_replicas);
	}
		else if ( strcmp (property, "mrsg_map_task_cost") == 0 )
	{
	    fscanf (file, "%lg", &config_mrsg.mrsg_map_task_cost);
	}
	else if ( strcmp (property, "mrsg_reduce_task_cost") == 0 )
	{
	    fscanf (file, "%lg", &config_mrsg.mrsg_reduce_task_cost);
	}
	else if ( strcmp (property, "mrsg_map_slots") == 0 )
	{
	    fscanf (file, "%d", &config_mrsg.mrsg_slots[MRSG_MAP]);
	}
	else if ( strcmp (property, "mrsg_reduces") == 0 )
	{
	    fscanf (file, "%d", &config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);
	}
	else if ( strcmp (property, "mrsg_reduce_slots") == 0 )
	{
	    fscanf (file, "%d", &config_mrsg.mrsg_slots[MRSG_REDUCE]);
	}
	else if ( strcmp (property, "mrsg_intermed_perc") == 0 )
	{
	    fscanf (file, "%lg", &config_mrsg.mrsg_perc);
	}
	else
	{
	    printf ("Error: Property %s is not valid. (in %s)", property, file_name);
	    exit (1);
	}
    }

    fclose (file);

    /* Assert the MRA configuration values. */

    xbt_assert (config_mra.mra_chunk_size > 0, "MRA_Chunk size must be greater than zero");
    xbt_assert (config_mra.mra_chunk_count > 0, "The amount of MRA_input chunks must be greater than zero");
    xbt_assert (config_mra.mra_chunk_replicas > 0, "The amount of MRA_chunk replicas must be greater than zero");
    xbt_assert (config_mra.mra_slots[MRA_MAP] > 0, "MRA_Map slots must be greater than zero");
    xbt_assert (config_mra.amount_of_tasks_mra[MRA_REDUCE] >= 0, "The number of MRA_reduce tasks can't be negative");
    xbt_assert (config_mra.mra_slots[MRA_REDUCE] > 0, "MRA_Reduce slots must be greater than zero");

    /* Assert the MRSG configuration values. */

    xbt_assert (config_mrsg.mrsg_chunk_size > 0, "Chunk size must be greater than zero");
    xbt_assert (config_mrsg.mrsg_chunk_count > 0, "The amount of input chunks must be greater than zero");
    xbt_assert (config_mrsg.mrsg_chunk_replicas > 0, "The amount of chunk replicas must be greater than zero");
    xbt_assert (config_mrsg.mrsg_slots[MRSG_MAP] > 0, "Map slots must be greater than zero");
    xbt_assert (config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE] >= 0, "The number of reduce tasks can't be negative");
    xbt_assert (config_mrsg.mrsg_slots[MRSG_REDUCE] > 0, "Reduce slots must be greater than zero");
    xbt_assert (config_mrsg.mrsg_perc > 0, "Intermediate percent must be greater than zero");
}



/**
 * @brief  Initialize the config structure in MRA.
 */
static void init_mra_config (void)
{
    const char*    process_name = NULL;
    msg_host_t     host;
    msg_process_t  process;
    size_t         mra_wid;
    unsigned int   cursor;
    w_mra_info_t       wi;
    xbt_dynar_t    process_list;

    /* Initialize MRA hosts information. */

    config_mra.mra_number_of_workers = 0;

    process_list = MSG_processes_as_dynar ();
    xbt_dynar_foreach (process_list, cursor, process)
    {
	process_name = MSG_process_get_name (process);
	if ( strcmp (process_name, "worker_mra") == 0 )
	    config_mra.mra_number_of_workers++;
    }

    config_mra.workers_mra = xbt_new (msg_host_t, config_mra.mra_number_of_workers);

    mra_wid = 0;
    config_mra.grid_cpu_power = 0.0;
    xbt_dynar_foreach (process_list, cursor, process)
    {
	process_name = MSG_process_get_name (process);
	host = MSG_process_get_host (process);
	if ( strcmp (process_name, "worker_mra") == 0 )
	{
	    config_mra.workers_mra[mra_wid] = host;
	    /* Set the worker ID as its data. */
	    wi = xbt_new (struct mra_w_info_s, 1);
	    wi->mra_wid = mra_wid;
	    MSG_host_set_data (host, (void*)wi);
	    /* Add the worker's cpu power to the grid total. */
	    config_mra.grid_cpu_power += MSG_host_get_speed (host);
	    mra_wid++;
	}
    }
    config_mra.grid_average_speed = config_mra.grid_cpu_power / config_mra.mra_number_of_workers;
    config_mra.mra_heartbeat_interval = mra_maxval (MRA_HEARTBEAT_MIN_INTERVAL, config_mra.mra_number_of_workers / 100);
    config_mra.amount_of_tasks_mra[MRA_MAP] = config_mra.mra_chunk_count;
    config_mra.initialized = 1;
}


/**
 * @brief  Initialize the config structure in MRSG.
 */
static void init_mrsg_config (void)
{
    const char*    process_name = NULL;
    msg_host_t     host;
    msg_process_t  process;
    size_t         mrsg_wid;
    unsigned int   cursor;
    w_mrsg_info_t       wi;
    xbt_dynar_t    process_list;

    /* Initialize MRSG hosts information. */

    config_mrsg.mrsg_number_of_workers = 0;

    process_list = MSG_processes_as_dynar ();
    xbt_dynar_foreach (process_list, cursor, process)
    {
	process_name = MSG_process_get_name (process);
	if ( strcmp (process_name, "worker_mrsg") == 0 )
	    config_mrsg.mrsg_number_of_workers++;
    }

    config_mrsg.workers_mrsg = xbt_new (msg_host_t, config_mrsg.mrsg_number_of_workers);

    mrsg_wid = 0;
    config_mrsg.grid_cpu_power = 0.0;
    xbt_dynar_foreach (process_list, cursor, process)
    {
	process_name = MSG_process_get_name (process);
	host = MSG_process_get_host (process);
	if ( strcmp (process_name, "worker_mrsg") == 0 )
	{
	    config_mrsg.workers_mrsg[mrsg_wid] = host;
	    /* Set the worker ID as its data. */
	    wi = xbt_new (struct mrsg_w_info_s, 1);
	    wi->mrsg_wid = mrsg_wid;
	    MSG_host_set_data (host, (void*)wi);
	    /* Add the worker's cpu power to the grid total. */
	    config_mrsg.grid_cpu_power += MSG_host_get_speed (host);
	    mrsg_wid++;
	}
    }
    config_mrsg.grid_average_speed = config_mrsg.grid_cpu_power / config_mrsg.mrsg_number_of_workers;
    config_mrsg.mrsg_heartbeat_interval = mrsg_maxval (MRSG_HEARTBEAT_MIN_INTERVAL, config_mrsg.mrsg_number_of_workers / 100);
    config_mrsg.amount_of_tasks_mrsg[MRSG_MAP] = config_mrsg.mrsg_chunk_count;
    config_mrsg.initialized = 1;
}

/**
 * @brief  Initialize the job structure in MRA.
 */
static void init_job_mra (void)
{
    int     i;
    size_t  mra_wid;

    xbt_assert (config_mra.initialized, "init_mra_config has to be called before init_job_mra");

    job_mra.finished = 0;
    job_mra.mra_heartbeats = xbt_new (struct mra_heartbeat_s, config_mra.mra_number_of_workers);

    //Workrs PID info
        mra_task_pid.listen = xbt_new(int,config_mra.mra_number_of_workers);
        mra_task_pid.data_node = xbt_new(int,config_mra.mra_number_of_workers);
        mra_task_pid.worker = xbt_new(int,config_mra.mra_number_of_workers);
        mra_task_pid.workers_on = config_mra.mra_number_of_workers;
        mra_task_pid.status = xbt_new(int,config_mra.mra_number_of_workers);

    for (mra_wid = 0; mra_wid < config_mra.mra_number_of_workers; mra_wid++)
    {
			job_mra.mra_heartbeats[mra_wid].slots_av[MRA_MAP] = config_mra.mra_slots[MRA_MAP];
			job_mra.mra_heartbeats[mra_wid].slots_av[MRA_REDUCE] = config_mra.mra_slots[MRA_REDUCE];
			job_mra.mra_heartbeats[mra_wid].wid_timestamp = sizeof(long double);
      mra_task_pid.status[mra_wid] = ON;

    }

    /* Initialize map information. */
    job_mra.tasks_pending[MRA_MAP] = config_mra.amount_of_tasks_mra[MRA_MAP];
    job_mra.task_status[MRA_MAP] = xbt_new0 (int, config_mra.amount_of_tasks_mra[MRA_MAP]);
    job_mra.task_instances[MRA_MAP] = xbt_new0 (int, config_mra.amount_of_tasks_mra[MRA_MAP]);
    job_mra.task_list[MRA_MAP] = xbt_new0 (msg_task_t*, config_mra.amount_of_tasks_mra[MRA_MAP]);
    job_mra.mra_task_dist[MRA_MAP] = xbt_new (int*, (config_mra.mra_number_of_workers * config_mra.amount_of_tasks_mra[MRA_MAP]) * (sizeof (int)));
    for (i = 0; i < config_mra.amount_of_tasks_mra[MRA_MAP] ; i++)
    {
	  job_mra.task_list[MRA_MAP][i] = xbt_new0 (msg_task_t, MAX_SPECULATIVE_COPIES);
    }
    for (i = 0; i < config_mra.mra_number_of_workers; i++)
    {
    job_mra.mra_task_dist[MRA_MAP][i] = xbt_new0 (int, (config_mra.mra_number_of_workers * config_mra.amount_of_tasks_mra[MRA_MAP]) * (sizeof (int)));
    }
    /* Initialize Reduce tasks number. */
    if (config_mra.Fg != 1) {
    		config_mra.amount_of_tasks_mra[MRA_REDUCE] = config_mra.Fg * config_mra.mra_number_of_workers;
    		}


    job_mra.map_output = xbt_new (size_t*, config_mra.mra_number_of_workers);
    for (i = 0; i < config_mra.mra_number_of_workers; i++)
	  job_mra.map_output[i] = xbt_new0 (size_t, config_mra.amount_of_tasks_mra[MRA_REDUCE]);

    // Initialize reduce information.

    job_mra.tasks_pending[MRA_REDUCE] = config_mra.amount_of_tasks_mra[MRA_REDUCE];
    job_mra.task_status[MRA_REDUCE] = xbt_new0 (int, config_mra.amount_of_tasks_mra[MRA_REDUCE]);
    job_mra.task_instances[MRA_REDUCE] = xbt_new0 (int, config_mra.amount_of_tasks_mra[MRA_REDUCE]);
    job_mra.task_list[MRA_REDUCE] = xbt_new0 (msg_task_t*, config_mra.amount_of_tasks_mra[MRA_REDUCE]);
    job_mra.mra_task_dist[MRA_REDUCE] = xbt_new (int*,  config_mra.amount_of_tasks_mra[MRA_REDUCE] * (sizeof (int)));
    for (i = 0; i < config_mra.amount_of_tasks_mra[MRA_REDUCE]; i++)
    {
	  job_mra.task_list[MRA_REDUCE][i] = xbt_new0 (msg_task_t, MAX_SPECULATIVE_COPIES);
	  }
	  for (i = 0; i < config_mra.mra_number_of_workers ; i++)
    {
	  job_mra.mra_task_dist[MRA_REDUCE][i] = xbt_new0 (int, (config_mra.mra_number_of_workers * config_mra.amount_of_tasks_mra[MRA_REDUCE]) * (sizeof (int)));
	  }
	 // Configuracao dos Reduces Termina aqui */

}


/**
 * @brief  Initialize the job structure in MRSG.
 */
static void init_job_mrsg (void)
{
    int     i;
    size_t  mrsg_wid;

    xbt_assert (config_mrsg.initialized, "init_config has to be called before init_job");

    job_mrsg.finished = 0;
    job_mrsg.mrsg_heartbeats = xbt_new (struct mrsg_heartbeat_s, config_mrsg.mrsg_number_of_workers);

    //Workrs PID info
        mrsg_task_pid.listen = xbt_new(int,config_mrsg.mrsg_number_of_workers);
        mrsg_task_pid.data_node = xbt_new(int,config_mrsg.mrsg_number_of_workers);
        mrsg_task_pid.worker = xbt_new(int,config_mrsg.mrsg_number_of_workers);
        mrsg_task_pid.workers_on = config_mrsg.mrsg_number_of_workers;
        mrsg_task_pid.status = xbt_new(int,config_mrsg.mrsg_number_of_workers);

    for (mrsg_wid = 0; mrsg_wid < config_mrsg.mrsg_number_of_workers; mrsg_wid++)
    {
	job_mrsg.mrsg_heartbeats[mrsg_wid].slots_av[MRSG_MAP] = config_mrsg.mrsg_slots[MRSG_MAP];
	job_mrsg.mrsg_heartbeats[mrsg_wid].slots_av[MRSG_REDUCE] = config_mrsg.mrsg_slots[MRSG_REDUCE];
  mrsg_task_pid.status[mrsg_wid] = ON;

    }

    /* Initialize map information. */
    job_mrsg.tasks_pending[MRSG_MAP] = config_mrsg.amount_of_tasks_mrsg[MRSG_MAP];
    job_mrsg.task_status[MRSG_MAP] = xbt_new0 (int, config_mrsg.amount_of_tasks_mrsg[MRSG_MAP]);
    job_mrsg.task_instances[MRSG_MAP] = xbt_new0 (int, config_mrsg.amount_of_tasks_mrsg[MRSG_MAP]);
    job_mrsg.task_list[MRSG_MAP] = xbt_new0 (msg_task_t*, config_mrsg.amount_of_tasks_mrsg[MRSG_MAP]);
    for (i = 0; i < config_mrsg.amount_of_tasks_mrsg[MRSG_MAP]; i++)
	job_mrsg.task_list[MRSG_MAP][i] = xbt_new0 (msg_task_t, MAX_SPECULATIVE_COPIES);

    job_mrsg.map_output = xbt_new (size_t*, config_mrsg.mrsg_number_of_workers);
    for (i = 0; i < config_mrsg.mrsg_number_of_workers; i++)
	job_mrsg.map_output[i] = xbt_new0 (size_t, config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);

    /* Initialize reduce information. */
    job_mrsg.tasks_pending[MRSG_REDUCE] = config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE];
    job_mrsg.task_status[MRSG_REDUCE] = xbt_new0 (int, config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);
    job_mrsg.task_instances[MRSG_REDUCE] = xbt_new0 (int, config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);
    job_mrsg.task_list[MRSG_REDUCE] = xbt_new0 (msg_task_t*, config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);
    for (i = 0; i < config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]; i++)
	job_mrsg.task_list[MRSG_REDUCE][i] = xbt_new0 (msg_task_t, MAX_SPECULATIVE_COPIES);

}


/**
 * @brief  Initialize the stats structure in MRA.
 */
static void init_mra_stats (void)
{
    xbt_assert (config_mra.initialized, "init_mra_config has to be called before init_mra_stats");

    stats_mra.map_local_mra 			= 0;
    stats_mra.mra_map_remote 			= 0;
    stats_mra.map_spec_mra_l 			= 0;
    stats_mra.map_spec_mra_r 			= 0;
    stats_mra.reduce_mra_normal 	= 0;
    stats_mra.reduce_mra_spec 		= 0;
    stats_mra.mra_map_recovery 		= 0;
    stats_mra.mra_reduce_recovery = 0;
}
/**
 * @brief  Initialize the stats structure in MRSG.
 */
static void init_mrsg_stats (void)
{
    xbt_assert (config_mrsg.initialized, "init_config has to be called before init_stats");

    stats_mrsg.map_local_mrsg 		= 0;
    stats_mrsg.map_remote_mrsg 		= 0;
    stats_mrsg.map_spec_mrsg_l 		= 0;
    stats_mrsg.map_spec_mrsg_r 		= 0;
    stats_mrsg.reduce_mrsg_normal = 0;
    stats_mrsg.reduce_mrsg_spec 	= 0;
}

void print_bighybrid_output()
{
  FILE * mra_results = fopen("stats_mra.csv","a");
  FILE * mrsg_results = fopen("stats_mrsg.csv","a");;
  FILE * time_mra = fopen("time_mra.csv","a");
  FILE * time_mrsg = fopen("time_mrsg.csv","a");
  fseek(mra_results, 0, SEEK_END);
  fseek(mrsg_results, 0, SEEK_END);

  fprintf(time_mra,"%d;%d;%d;%f;%f;%f\n",(int)config_mra.perc_vc_node,config_mra.Fg,config_mra.mra_chunk_count,config_mra.mra_chunk_size,
  stats_mra.map_time,stats_mra.reduce_time );
  fprintf(time_mrsg,"%d;%f;%f;%f\n",config_mrsg.mrsg_chunk_count,config_mrsg.mrsg_chunk_size,stats_mrsg.map_time,stats_mrsg.reduce_time);

  fprintf(mra_results, "%f;%d;%d;%f",config_mra.perc_vc_node,config_mra.Fg,config_mra.mra_chunk_count,config_mra.mra_chunk_size );
  fprintf(mrsg_results, "%d;%f",config_mrsg.mrsg_chunk_count,config_mrsg.mrsg_chunk_size );
  fprintf(mra_results,"%d;%d;%d;%d;%d;%d;%d;%d;%d;%d;%f;%f\n",
          stats_mra.map_local_mra,stats_mra.mra_map_remote,stats_mra.map_spec_mra_l,
          stats_mra.map_spec_mra_r,stats_mra.mra_map_recovery,
          stats_mra.mra_map_remote + stats_mra.map_spec_mra_r,
          stats_mra.map_spec_mra_l + stats_mra.map_spec_mra_r,
          stats_mra.reduce_mra_normal,stats_mra.reduce_mra_spec,
          stats_mra.mra_reduce_recovery,stats_mra.map_time,
          stats_mra.reduce_time);

  fprintf(mrsg_results,"%d;%d;%d;%d;%d;%d;%d;%d;%f;%f\n",
          stats_mrsg.map_local_mrsg,stats_mrsg.map_remote_mrsg,stats_mrsg.map_spec_mrsg_l
          ,stats_mrsg.map_spec_mrsg_r,stats_mrsg.map_remote_mrsg + stats_mrsg.map_spec_mrsg_r
          ,stats_mrsg.map_spec_mrsg_l + stats_mrsg.map_spec_mrsg_r,stats_mrsg.reduce_mrsg_normal,
          stats_mrsg.reduce_mrsg_spec,stats_mrsg.map_time,stats_mrsg.reduce_time);

  fclose(mrsg_results);
  fclose(mra_results);
  fclose(time_mra);
  fclose(time_mrsg);
}

/**
 * @brief  Free allocated memory for global variables in MRA.
 */
static void free_mra_global_mem (void)
{
    size_t  i;

for (i = 0; i < config_mra.mra_chunk_count; i++)
	xbt_free_ref (&chunk_owner_mra[i]);
    xbt_free_ref (&chunk_owner_mra);

    xbt_free_ref (&config_mra.workers_mra);
    xbt_free_ref (&job_mra.task_status[MRA_MAP]);
    xbt_free_ref (&job_mra.task_instances[MRA_MAP]);
    xbt_free_ref (&job_mra.task_status[MRA_REDUCE]);
    xbt_free_ref (&job_mra.task_instances[MRA_REDUCE]);
    xbt_free_ref (&job_mra.mra_heartbeats);
    for (i = 0; i < config_mra.amount_of_tasks_mra[MRA_MAP]; i++)
	xbt_free_ref (&job_mra.task_list[MRA_MAP][i]);
    xbt_free_ref (&job_mra.task_list[MRA_MAP]);
    for (i = 0; i < config_mra.amount_of_tasks_mra[MRA_REDUCE]; i++)
	xbt_free_ref (&job_mra.task_list[MRA_REDUCE][i]);
    xbt_free_ref (&job_mra.task_list[MRA_REDUCE]);
    for (i = 0; i < config_mra.mra_number_of_workers; i++)
    {
      xbt_free_ref (&job_mra.mra_task_dist[MRA_REDUCE][i]);
      xbt_free_ref (&job_mra.mra_task_dist[MRA_MAP][i]);
    }

    xbt_free_ref(&mra_task_pid.worker);
    xbt_free_ref(&mra_task_pid.data_node);
    xbt_free_ref(&mra_task_pid.listen);
    xbt_free_ref(&mra_task_pid.status);
}


/**
 * @brief  Free allocated memory for global variables MRSG.
 */
static void free_mrsg_global_mem (void)
{
    size_t  imrsg;

    for (imrsg = 0; imrsg < config_mrsg.mrsg_chunk_count; imrsg++)
	xbt_free_ref (&chunk_owner_mrsg[imrsg]);
    xbt_free_ref (&chunk_owner_mrsg);

    xbt_free_ref (&config_mrsg.workers_mrsg);
    xbt_free_ref (&job_mrsg.task_status[MRSG_MAP]);
    xbt_free_ref (&job_mrsg.task_instances[MRSG_MAP]);
    xbt_free_ref (&job_mrsg.task_status[MRSG_REDUCE]);
    xbt_free_ref (&job_mrsg.task_instances[MRSG_REDUCE]);
    xbt_free_ref (&job_mrsg.mrsg_heartbeats);
    for (imrsg = 0; imrsg < config_mrsg.amount_of_tasks_mrsg[MRSG_MAP]; imrsg++)
	xbt_free_ref (&job_mrsg.task_list[MRSG_MAP][imrsg]);
    xbt_free_ref (&job_mrsg.task_list[MRSG_MAP]);
    for (imrsg = 0; imrsg < config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]; imrsg++)
	xbt_free_ref (&job_mrsg.task_list[MRSG_REDUCE][imrsg]);
    xbt_free_ref (&job_mrsg.task_list[MRSG_REDUCE]);

    xbt_free_ref(&mrsg_task_pid.worker);
    xbt_free_ref(&mrsg_task_pid.data_node);
    xbt_free_ref(&mrsg_task_pid.listen);
    xbt_free_ref(&mrsg_task_pid.status);
}
/*static void finish_all_pids (void)
{
int 	k, ftm_pid;

for (k=0; k < total_tasks; k++)
{
ftm_pid = mra_task_ftm[k].mra_ft_pid[MRA_MAP];
MSG_process_killall (ftm_pid);
}

}*/
