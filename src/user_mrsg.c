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
#include "dfs_mrsg.h"
#include "bighybrid.h"


void MRSG_user_init (void)
{
    user_mrsg.task_cost_f = NULL;
    user_mrsg.dfs_f = default_mrsg_dfs_f;
    user_mrsg.map_output_f = NULL;
}

void MRSG_set_task_cost_f ( double (*f)(enum mrsg_phase_e mrsg_phase, size_t tid, size_t mrsg_wid) )
{
    user_mrsg.task_cost_f = f;
}

void MRSG_set_dfs_f ( void (*f)(char** dfs_matrix, size_t chunks, size_t workers_mrsg, int replicas) )
{
    user_mrsg.dfs_f = f;
}

void MRSG_set_map_output_f ( int (*f)(size_t mid, size_t rid) )
{
    user_mrsg.map_output_f = f;
}

