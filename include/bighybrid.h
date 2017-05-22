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

#ifndef BIGHYBRID_H
#define BIGHYBRID_H


#include <stdlib.h>


/** @brief  Possible execution phases. */
enum mra_phase_e {
    MRA_MAP,
    MRA_REDUCE
};

enum mrsg_phase_e {
    MRSG_MAP,
    MRSG_REDUCE
};




int BIGHYBRID_main (const char* plat, const char* depl, const char* conf, const char* vc_file);

// MRA setup
void MRA_user_init (void);

void MRA_set_task_mra_cost_f ( double (*f)(enum mra_phase_e mra_phase, size_t tid_mra, size_t mra_wid) );

void MRA_set_dfs_f ( void (*f)(char** mra_dfs_matrix, size_t chunks, size_t workers_mra, int replicas) );

void MRA_set_map_mra_output_f ( int (*f)(size_t mra_mid, size_t mra_rid) );

// MRSG setup
void MRSG_user_init (void);

void MRSG_set_task_cost_f ( double (*f)(enum mrsg_phase_e mrsg_phase, size_t tid_mrsg, size_t mrsg_wid) );

void MRSG_set_dfs_f ( void (*f)(char** dfs_matrix, size_t chunks, size_t workers_mrsg, int replicas) );

void MRSG_set_map_output_f ( int (*f)(size_t mrsg_mid, size_t mrsg_rid) );



#endif /* !BIGHYBRID_H */
