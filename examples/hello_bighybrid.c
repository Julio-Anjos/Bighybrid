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


#include "common_bighybrid.h"
#include "mra_cv.h"
#include <bighybrid.h>


void BigHybrid_init ();

static void read_mra_config_file (const char* file_name)
{
    char    property[256];
    FILE*   file;

    /* Set the default configuration. */
    config_mra.mra_chunk_size = 67108864;
    config_mra.amount_of_tasks_mra[MRA_REDUCE] = 1;
    config_mra.Fg=1;
    config_mra.mra_perc=100;

    /* Read the user configuration file. */

    file = fopen (file_name, "r");
    /* Read the user configuration file. */

    xbt_assert (file != NULL, "Error reading cofiguration file: %s", file_name);

    while ( fscanf (file, "%256s", property) != EOF )
    {
			if ( strcmp (property, "mra_chunk_size") == 0 )
			{
	    fscanf (file, "%lg", &config_mra.mra_chunk_size);
	    config_mra.mra_chunk_size *= 1024 * 1024; /* MB -> bytes */
			}
			else if ( strcmp (property, "grain_factor") == 0 )
			{
	    fscanf (file, "%d", &config_mra.Fg);
			}
			else if ( strcmp (property, "mra_intermed_perc") == 0 )
			{
	    fscanf (file, "%lg", &config_mra.mra_perc);
			}
			else if ( strcmp (property, "mra_map_task_cost") == 0 )
			{
	    	fscanf (file, "%lg", &config_mra.mra_map_task_cost);
			}
			else if ( strcmp (property, "mra_reduce_task_cost") == 0 )
			{
	    fscanf (file, "%lg", &config_mra.mra_reduce_task_cost);
			}
			else if ( strcmp (property, "mra_reduces") == 0 )
			{
	    fscanf (file, "%d", &config_mra.amount_of_tasks_mra[MRA_REDUCE]);
			}
			else
			{
	    printf ("Error: Property %s is not valid. (in %s)", property, file_name);
	    exit (1);
			}

    }

    fclose (file);

}

static void read_mrsg_config_file (const char* file_name)
{
    char    property[256];
    FILE*   file;

    /* Set the default configuration. */
    config_mrsg.mrsg_chunk_size = 67108864;
    config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE] = 1;
    config_mrsg.mrsg_slots[MRSG_REDUCE] = 2;
    config_mrsg.mrsg_perc = 100;


    /* Read the user configuration file. */

    file = fopen (file_name, "r");

    xbt_assert (file != NULL, "Error reading cofiguration file: %s", file_name);

    while ( fscanf (file, "%256s", property) != EOF )
    {
			if ( strcmp (property, "mrsg_chunk_size") == 0 )
				{
	    		fscanf (file, "%lg", &config_mrsg.mrsg_chunk_size);
	    		config_mrsg.mrsg_chunk_size *= 1024 * 1024; /* MB -> bytes */
				}
			else if ( strcmp (property, "mrsg_reduces") == 0 )
				{
	    		fscanf (file, "%d", &config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);
				}
				else if ( strcmp (property, "mrsg_map_task_cost") == 0 )
				{
	    		fscanf (file, "%lg", &config_mrsg.mrsg_map_task_cost);
				}
				else if ( strcmp (property, "mrsg_reduce_task_cost") == 0 )
				{
	    		fscanf (file, "%lg", &config_mrsg.mrsg_reduce_task_cost);
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
}


/**
 * User function that indicates the amount of bytes
 * that a map task will emit to a reduce task.
 *
 * @param  mra_mid  The ID of the map task.
 * @param  mra_rid  The ID of the reduce task.
 * @return The amount of data emitted (in bytes).
 */
int mra_map_output_function (size_t mra_mid, size_t mra_rid)
{

		return ((config_mra.mra_chunk_size*config_mra.mra_perc/100)/config_mra.amount_of_tasks_mra[MRA_REDUCE]);
//     return 2*1024*1024;
}

/**
 * User function that indicates the amount of bytes
 * that a map task will emit to a reduce task.
 *
 * @param  mrsg_mid  The ID of the map task.
 * @param  mrsg_rid  The ID of the reduce task.
 * @return The amount of data emitted (in bytes).
 */

int mrsg_map_output_function (size_t mrsg_mid, size_t mrsg_rid)
{

		return ((config_mrsg.mrsg_chunk_size*config_mrsg.mrsg_perc/100)/config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);

//    return 4*1024*1024;
}



/**
 * User function that indicates the MRA cost of a task.
 *
 * @param  mra_phase  The execution phase.
 * @param  tid_mra    The ID of the task.
 * @param  mra_wid    The ID of the worker that received the task.
 * @return The task cost in FLOPs.
 */
double mra_task_cost_function (enum mra_phase_e mra_phase, size_t tid_mra, size_t mra_wid)
{
   double mra_map_required;
   double mra_reduce_required;

    switch (mra_phase)
    {
	case MRA_MAP:
	    config_mra.cpu_required_map_mra = config_mra.mra_map_task_cost * config_mra.mra_chunk_size/(1024 * 1024);
      mra_map_required = config_mra.cpu_required_map_mra/config_mra.mra_slots[MRA_MAP];
	    return mra_map_required;

	case MRA_REDUCE:
	    config_mra.cpu_required_reduce_mra = config_mra.mra_reduce_task_cost* ((config_mra.mra_chunk_size/(1024 * 1024) *config_mra.mra_perc/100)/config_mra.amount_of_tasks_mra[MRA_REDUCE]);
      mra_reduce_required = config_mra.cpu_required_reduce_mra/config_mra.mra_slots[MRA_REDUCE];
      if (config_mra.Fg >1)
      {
        mra_reduce_required *= 1/config_mra.Fg;
      }
	    return mra_reduce_required;
    }

}

/**
 * User function that indicates the MRSG cost of a task.
 *
 * @param  mrsg_phase  The execution phase.
 * @param  tid_mrsg    The ID of the task.
 * @param  mrsg_wid    The ID of the worker that received the task.
 * @return The task cost in FLOPs.
 */

double mrsg_task_cost_function (enum mrsg_phase_e mrsg_phase, size_t tid_mrsg, size_t mrsg_wid)
{
   double mrsg_map_required;
   double mrsg_reduce_required;

    switch (mrsg_phase)
    {
	case MRSG_MAP:
	    config_mrsg.cpu_required_map_mrsg = config_mrsg.mrsg_map_task_cost * config_mrsg.mrsg_chunk_size/(1024 * 1024);
      mrsg_map_required = config_mrsg.cpu_required_map_mrsg/config_mrsg.mrsg_slots[MRSG_MAP];
	    return mrsg_map_required;

	case MRSG_REDUCE:
	    config_mrsg.cpu_required_reduce_mrsg = config_mrsg.mrsg_reduce_task_cost* ((config_mrsg.mrsg_chunk_size/(1024 * 1024) *config_mrsg.mrsg_perc/100)/config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);
      mrsg_reduce_required = config_mrsg.cpu_required_reduce_mrsg/config_mrsg.mrsg_slots[MRSG_REDUCE];
	    return mrsg_reduce_required;
    }

}


int main (int argc, char* argv[])
{
    /* MRA_user_init must be called before setting the user functions. */
    MRA_user_init ();
    /* MRSG_user_init must be called before setting the user functions. */
    MRSG_user_init ();
    /* Set the task cost MRA function. */
    MRA_set_task_mra_cost_f (mra_task_cost_function);
    /* Set the task cost MRSG function. */
    MRSG_set_task_cost_f (mrsg_task_cost_function);
    /* Set the MRA_map output function. */
    MRA_set_map_mra_output_f (mra_map_output_function);
    /* Set the MRSG_map output function. */
    MRSG_set_map_output_f (mrsg_map_output_function);


    /* Run the MRSG simulation. */
     //BIGHYBRID_main ("plat350-350.xml","d-plat350-350.xml","bighyb-plat350-350.conf","parser-boinc-080.txt");

     /*
       Check the args given in the command line:
         < 5 : invalid (something is missing)
         = 5 : no simgrid arg
         > 5 : simgrid arg
     */
     if(argc < 5)
     {
     	printf("usage : %s plat.xml depoly.xml plat.conf machines-trace.txt",argv[0]);
     	exit(0);
     }else if (argc == 5){
      int sg_argc = argc -4;
      MSG_init (&sg_argc, argv);
      printf("\n%s %s %s %s",argv[argc-4],argv[argc-3],argv[argc-2],argv[argc-1]);
      BIGHYBRID_main (argv[argc-4],argv[argc-3],argv[argc-2],argv[argc-1]);

     }else{
       int sg_argc = argc -4;
    //	printf("WITH SIMGRID %s\n%s %s %s %s\n",argv[1],argv[sg_argc],argv[sg_argc+1], argv[sg_argc+2],argv[sg_argc+3]);
       MSG_init (&sg_argc, argv);
      // BIGHYBRID_main (argv[sg_argc+1],argv[sg_argc+2], argv[sg_argc+3],argv[sg_argc+4]);
       BIGHYBRID_main (argv[sg_argc],argv[sg_argc+1], argv[sg_argc+2],argv[sg_argc+3]);
     }

    return 0;
}
