#!/bin/bash
export LD_LIBRARY_PATH=$HOME/simgrid-3.14.159/lib

MRSG_NET=()
MRSG_LAT=()
MRA_NET=()
MRA_LAT=()
MRA_MACHINES=()
MRSG_MACHINES=()
MSRG_CHUNK_COUNT=()
MRA_CHUNK_COUNT=()
MRA_CHUNK_SIZE=()
MRSG_CHUNK_SIZE=()
MRSG_MAP_COST=()
MRSG_REDUCE_COST=()
MRA_MAP_COST=()
MRA_REDUCE_COST=()
PERC_VOLATILITE=()
LOAD_OUT=()
while read p; do
	case $p in
		"MRA Machines"*) MRA_MACHINES+=($(echo $p | grep -o "[0-9]*"));;
		"MRSG Machines"*) MRSG_MACHINES+=($(echo $p | grep -o "[0-9]*"));;
		"Load Cloud"*) MSRG_CHUNK_COUNT+=($(echo $p | grep -o "[0-9]*"));;
		"Load DG"*) MRA_CHUNK_COUNT+=($(echo $p | grep -o "[0-9]*"));;
		"MRA Map Cost"*) MRA_MAP_COST+=($(echo $p | grep -o "[0-9]*.[0-9]*e+[0-9]*"));;
		"MRA Reduce Cost"*) MRA_REDUCE_COST+=($(echo $p | grep -o "[0-9]*.[0-9]*e+[0-9]*"));;
		"MRSG Map Cost"*) MRSG_MAP_COST+=($(echo $p | grep -o "[0-9]*.[0-9]*e+[0-9]*"));;
		"MRSG Reduce Cost"*)MRSG_REDUCE_COST+=($(echo $p | grep -o "[0-9]*.[0-9]*e+[0-9]*"));;
		"Vol"*)PERC_VOLATILITE+=($(echo $p | grep -o "[0-9]*"));;
		"MRA chunk"*)MRA_CHUNK_SIZE+=($(echo $p | grep -o "[0-9]*"));;
		"MRSG chunk"*)MRSG_CHUNK_SIZE+=($(echo $p | grep -o "[0-9]*"));;
		"Load out"*)LOAD_OUT+=($(echo $p | grep -o "[0-9]*"));;
		"MRSG net"*)MRSG_NET+=($(echo $p | grep -o "[0-9]*.[0-9]*e+[0-9]*"));;
		"MRSG lat"*)MRSG_LAT+=($(echo $p | grep -o "[0-9]*.[0-9]*e-[0-9]*"));;
		"MRA net"*)MRA_NET+=($(echo $p | grep -o "[0-9]*.[0-9]*e+[0-9]*"));;
		"MRA lat"*)MRA_LAT+=($(echo $p | grep -o "[0-9]*.[0-9]*e-[0-9]*"));;
	esac

done < experimentos.txt


numberExperiments=${#MRA_MACHINES[@]}
clear
cd ..
make clean all
cd examples/
make clean all
rm times.txt
touch times.txt
for e in $(seq 0 `expr $numberExperiments - 1`)
do
	for n in $(seq 0 2)
	do
		WORKLOAD="Experiment_$e.$n"
		loadIN=$(echo "${MRA_CHUNK_COUNT[e]}+${MSRG_CHUNK_COUNT[e]}"| bc -l)
		MRSG_CHUNKS=$(echo "${MSRG_CHUNK_COUNT[e]}/64"| bc -l)
		MRA_CHUNKS=$(echo "${MRA_CHUNK_COUNT[e]}/64"| bc -l)
		MRSG_CHUNKS=${MRSG_CHUNKS%.*}
		MRA_CHUNKS=${MRA_CHUNKS%.*}

		INTERMED_PERC=$(echo "${LOAD_OUT[e]}/$loadIN*100"| bc -l)

		python create-bighybrid-plat.py $WORKLOAD.xml ${MRA_MACHINES[e]} 2 4e9:6e9 ${MRA_LAT[e]} ${MRA_NET[e]} ${MRSG_MACHINES[e]} 2 5e9 ${MRSG_LAT[e]} ${MRSG_NET[e]}
		python create-bighybrid-depoly.py $WORKLOAD.xml

		echo "	mrsg_reduces `expr ${MRSG_MACHINES[e]} \* 2`
		mrsg_chunk_size 64
		mrsg_input_chunks $MRSG_CHUNKS
		mrsg_dfs_replicas 3
		mrsg_map_slots 2
		mrsg_reduce_slots 2
		mrsg_intermed_perc $INTERMED_PERC 
		mrsg_map_task_cost ${MRSG_MAP_COST[e]}
		mrsg_reduce_task_cost ${MRSG_REDUCE_COST[e]}

		mra_reduces `expr ${MRA_MACHINES[e]} \* 2`
		mra_chunk_size 64
		mra_input_chunks $MRA_CHUNKS
		mra_dfs_replicas 5
		mra_map_slots 2
		mra_reduce_slots 2
		mra_intermed_perc $INTERMED_PERC
		perc_num_volatile_node ${PERC_VOLATILITE[e]}
		grain_factor 1
		failure_timeout 4.0
		mra_map_task_cost ${MRA_MAP_COST[e]}
		mra_reduce_task_cost ${MRA_REDUCE_COST[e]}" > $WORKLOAD.conf
			./hello_bighybrid.bin --cfg=tracing:no  $WORKLOAD.xml d-$WORKLOAD.xml $WORKLOAD.conf 2>&1| $HOME/simgrid-3.14.159/bin/colorize > $WORKLOAD.txt 
		rm -f $WORKLOAD.xml
		rm -f d-$WORKLOAD.xml
		echo $WORKLOAD >> times.txt
		cat $WORKLOAD.txt | grep "Map Time: " >> times.txt
		cat $WORKLOAD.txt | grep "Reduce Time: " >> times.txt
	done
done
#python create-bighybrid-plat.py fb.xml 3000 2 5e9 1e-4 1.25e8
#python create-bighybrid-depoly.py yh.xml
#python create-bighybrid-depoly.py fb.xml
#./hello_bighybrid.bin --cfg=tracing:no  cc.xml d-cc.xml cc.conf 2>&1| $HOME/simgrid-3.14.159/bin/colorize  > cc.txt &
#./hello_bighybrid.bin --cfg=tracing:no  yh.xml d-yh.xml yh_206.conf 2>&1| $HOME/simgrid-3.14.159/bin/colorize > yh_206.txt &
#./hello_bighybrid.bin --cfg=tracing:no  yh.xml d-yh.xml yh_568.conf  2>&1| $HOME/simgrid-3.14.159/bin/colorize > yh_568.txt &
#./hello_bighybrid.bin --cfg=tracing:no  fb.xml d-fb.xml fb.conf  2>&1| $HOME/simgrid-3.14.159/bin/colorize > fb.txt &
