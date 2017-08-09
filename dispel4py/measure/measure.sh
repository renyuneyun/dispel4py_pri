#!/bin/sh

trap "exit;" SIGINT

if [ ! -z $1 ]; then
	platform=$1
else
	platform=unspecified_platform
fi
version=opt1.1_
run_id=`date +%Y-%m-%d.%H:%M:%S`

static=false
iteration_min=1
iteration_max=100
iteration_step=10
num_repeat=8

source ~/self/Edinburgh/venv/dissertation/bin/activate

measure_dir=$PWD/measure
mkdir -p "$measure_dir"

overall_file="$measure_dir/overall"
if [ ! -f "$overall_file" ]; then
	echo workflow platform version run_id module number_of_iteration np max_number_of_sieves max_prime time | tee "$overall_file"
fi

function step {
	np=$1
	number_of_iteration=$2

	max_number_of_sieves=$3
	max_prime=$4

	echo np:$np number_of_iteration:$number_of_iteration max_number_of_sieves:$max_number_of_sieves max_prime:$max_prime

	fn_t_mpi=time_mpi
	fn_t_mpi_inc=time_mpi_inc

	wf_mpi=repeatable_prime_sieve__static
	wf_use_mpi=dispel4py.measure.graph.${wf_mpi}_$max_number_of_sieves
	if [ ! $static ]; then
		wf_mpi_inc=repeatable_prime_sieve
		wf_use_mpi_inc=dispel4py.measure.graph.${wf_mpi_inc}_$max_prime
	else
		wf_mpi_inc=$wf_mpi
		wf_use_mpi_inc=$wf_use_mpi
	fi

	np_mpi=$((max_number_of_sieves+1))
	if [ ! $static ]; then
		np_mpi_inc=$np
	else
		np_mpi_inc=$((np_mpi+1))
	fi

	exec_mpi="/usr/bin/time -o $fn_t_mpi -f %e mpiexec -np $np_mpi dispel4py mpi $wf_use_mpi -i $number_of_iteration"
	exec_mpi_inc="/usr/bin/time -o $fn_t_mpi_inc -f %e mpiexec -np $np_mpi_inc dispel4py mpi_inc $wf_use_mpi_inc -i $number_of_iteration"

	wd=$measure_dir/`date +%Y-%m-%d.%H:%M:%S` &&
	mkdir -p "$wd" &&
	cd "$wd" &&
	mkdir -p outputs/mpi{,_inc} &&

	echo Number of initial nodes: $np > configure &&
	echo Number of iterations: $number_of_iteration >> configure &&

	echo $exec_mpi_inc &&
	eval $exec_mpi_inc > stdout_mpi_inc 2> stderr_mpi_inc &&
	mpi_inc_time=`cat $fn_t_mpi_inc | tr -d '\n'` &&
	echo $wf_mpi_inc $platform $version $run_id mpi_inc $number_of_iteration $np_mpi_inc $max_number_of_sieves $max_prime $mpi_inc_time | tee -a "$overall_file" &&

	echo $exec_mpi &&
	eval $exec_mpi > stdout_mpi 2> stderr_mpi &&
	mpi_time=`cat $fn_t_mpi | tr -d '\n'` &&
	echo $wf_mpi $platform $version $run_id mpi $number_of_iteration $np_mpi $max_number_of_sieves $max_prime $mpi_time | tee -a "$overall_file"
}

all=(
"26 100"
"168 1000"
"303 2000"
"430 3000"
"550 4000"
"669 5000"
"783 6000"
"900 7000"
"1007 8000"
"1117 9000"
"1397 10000"
)

np=3

echo Ready

length=${#all[@]}
for ((i=0;i<$length;i++)) do
	pair=${all[i]}
	for ((number_of_iteration=$iteration_min;number_of_iteration<=$iteration_max;number_of_iteration+=$iteration_step)); do
		for ((iter=0;iter<$num_repeat;iter+=1)); do
			echo step $np $number_of_iteration $pair
		done
	done
done

deactivate

