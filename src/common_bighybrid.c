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


XBT_LOG_EXTERNAL_DEFAULT_CATEGORY (msg_test);

msg_error_t mra_send (const char* str, double cpu, double net, void* data, const char* mailbox)
{
    msg_error_t  status_mra;
    msg_task_t   msg_mra = NULL;

    msg_mra = MSG_task_create (str, cpu, net, data);

#ifdef VERBOSE
    if (!mra_message_is (msg_mra, SMS_HEARTBEAT_MRA))
	    XBT_INFO ("TX (%s): %s", mailbox, str);
#endif

    status_mra = MSG_task_send (msg_mra, mailbox);

#ifdef VERBOSE
    if (status_mra != MSG_OK)
	XBT_INFO ("ERROR %d SENDING MESSAGE: %s", status_mra, str);
#endif

    return status_mra;
}

msg_error_t send (const char* str, double cpu, double net, void* data, const char* mailbox)
{
    msg_error_t  status;
    msg_task_t   msg = NULL;

    msg = MSG_task_create (str, cpu, net, data);

#ifdef VERBOSE
    if (!mrsg_message_is (msg, SMS_HEARTBEAT_MRSG))
	    XBT_INFO ("TX (%s): %s", mailbox, str);
#endif

    status = MSG_task_send (msg, mailbox);

#ifdef VERBOSE
    if (status != MSG_OK)
	XBT_INFO ("ERROR %d SENDING MESSAGE: %s", status, str);
#endif

    return status;
}

msg_error_t send_mra_sms (const char* str, const char* mailbox)
{
    return mra_send (str, 0.0, 0.0, NULL, mailbox);
}

msg_error_t send_mrsg_sms (const char* str, const char* mailbox)
{
    return send (str, 0.0, 0.0, NULL, mailbox);
}


msg_error_t mra_receive (msg_task_t* msg_mra, const char* mailbox)
{
    msg_error_t  status_mra;

    status_mra = MSG_task_receive (msg_mra, mailbox);

#ifdef VERBOSE
    if (status_mra != MSG_OK)
	XBT_INFO ("ERROR %d RECEIVING MESSAGE", status_mra);
#endif

    return status_mra;
}


msg_error_t receive (msg_task_t* msg, const char* mailbox)
{
    msg_error_t  status;

    status = MSG_task_receive (msg, mailbox);

#ifdef VERBOSE
    if (status != MSG_OK)
	XBT_INFO ("ERROR %d RECEIVING MESSAGE", status);
#endif

    return status;
}

int mra_message_is (msg_task_t msg_mra, const char* str)
{
    if (strcmp (MSG_task_get_name (msg_mra), str) == 0)
	return 1;

    return 0;
}

int mrsg_message_is (msg_task_t msg, const char* str)
{
    if (strcmp (MSG_task_get_name (msg), str) == 0)
	return 1;

    return 0;
}

int mra_maxval (int mra_a, int mra_b)
{
    if (mra_b > mra_a)
	return mra_b;

    return mra_a;
}

int mrsg_maxval (int mrsg_a, int mrsg_b)
{
    if (mrsg_b > mrsg_a)
	return mrsg_b;

    return mrsg_a;
}

/**
 * @brief  Return the output size of a map task.
 * @param  mra_mid  The map task ID.
 * @return The task output size in bytes.
 */
size_t map_mra_output_size (size_t mra_mid)
{
    size_t  mra_rid;
    size_t  mra_sum = 0;

    for (mra_rid = 0; mra_rid < config_mra.amount_of_tasks_mra[MRA_REDUCE]; mra_rid++)
    {
	mra_sum += (user_mra.map_mra_output_f (mra_mid, mra_rid));
	  }

    return mra_sum;
}

/**
 * @brief  Return the output size of a map task.
 * @param  mrsg_mid  The map task ID.
 * @return The task output size in bytes.
 */
size_t map_mrsg_output_size (size_t mrsg_mid)
{
    size_t  mrsg_rid;
    size_t  mrsg_sum = 0;

    for (mrsg_rid = 0; mrsg_rid < config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]; mrsg_rid++)
    {
	mrsg_sum += user_mrsg.map_output_f (mrsg_mid, mrsg_rid);
    }

    return mrsg_sum;
}

/**
 * @brief  Return the input size of a reduce task.
 * @param  mra_rid  The reduce task ID.
 * @return The task input size in bytes.
 */
size_t reduce_mra_input_size (size_t mra_rid)
{
    size_t  mra_mid;
    size_t  mra_sum = 0;

    for (mra_mid = 0; mra_mid < config_mra.amount_of_tasks_mra[MRA_MAP]; mra_mid++)
    {
	mra_sum += (user_mra.map_mra_output_f (mra_mid, mra_rid));
    }
  XBT_INFO (" MRA_Reduce task %zu sent %zu Bytes",mra_rid,mra_sum);
    return mra_sum;
}

/**
 * @brief  Return the input size of a reduce task.
 * @param  mrsg_rid  The reduce task ID.
 * @return The task input size in bytes.
 */
size_t reduce_mrsg_input_size (size_t mrsg_rid)
{
    size_t  mrsg_mid;
    size_t  mrsg_sum = 0;

    for (mrsg_mid = 0; mrsg_mid < config_mrsg.amount_of_tasks_mrsg[MRSG_MAP]; mrsg_mid++)
    {
	mrsg_sum += user_mrsg.map_output_f (mrsg_mid, mrsg_rid);
    }
  XBT_INFO (" MRSG_Reduce task %zu sent %zu Bytes",mrsg_rid,mrsg_sum);
    return mrsg_sum;
}

/* @brief - reads the plataform bandwidth
*  @param plat file name
*/
void read_bandwidth_mra (const char* plat)
{

		FILE * xml = fopen(plat,"r");
    if(xml==NULL)
    {
      printf("Error: plat file not found\n");
      exit(1);
    }
	char buff[255];
	char * token;

	const char s[2]= {'"','\0'};
	do{
		 fscanf(xml, "%s", buff);
   	 token = strtok(buff,"=");
   	 if(!strcmp(token,"bandwidth"))
   	 {

		 	token = strtok(NULL,"=");
		 	token = strtok(token,s);
		 	 sscanf(token, "%lf", &config_mra.mra_bandwidth);
 		 	break;
		 }
	}while(fgetc(xml)!=EOF);
	fclose(xml);
//	printf("\n\nBandwidth:%f\n\n",config_mra.mra_bandwidth);
}
