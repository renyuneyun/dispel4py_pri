#!/bin/sh

trap "exit;" SIGINT

platform=$1
version=opt1.1_
run_id=`date +%Y-%m-%d.%H:%M:%S`

source ~/self/Edinburgh/venv/dissertation/bin/activate

measure_dir=$PWD/measure
mkdir -p "$measure_dir"

overall_file="$measure_dir/overall"
if [ ! -f "$overall_file" ]; then
	echo platform version run_id number_of_iteration np max_number_of_sieves max_prime mpi_time mpi_inc_time | tee "$overall_file"
fi

function step {
	np=$1
	number_of_iteration=$2

	max_number_of_sieves=$3
	max_prime=$4

	echo np:$np number_of_iteration:$number_of_iteration max_number_of_sieves:$max_number_of_sieves max_prime:$max_prime

	fn_t_mpi=time_mpi
	fn_t_mpi_inc=time_mpi_inc

	wd=$measure_dir/`date +%Y-%m-%d.%H:%M:%S` &&
	mkdir -p "$wd" &&
	cd "$wd" &&
	mkdir -p outputs/mpi{,_inc} &&

	echo Number of initial nodes: $np > configure &&
	echo Number of iterations: $number_of_iteration >> configure &&

	echo /usr/bin/time -o $fn_t_mpi_inc -f %e mpiexec -np $np dispel4py mpi_inc dispel4py.measure.graph.repeatable_prime_sieve_$max_prime -i $number_of_iteration &&
	/usr/bin/time -o $fn_t_mpi_inc -f %e mpiexec -np $np dispel4py mpi_inc dispel4py.measure.graph.repeatable_prime_sieve_$max_prime -i $number_of_iteration > stdout_mpi_inc 2> stderr_mpi_inc &&

	echo /usr/bin/time -o $fn_t_mpi -f %e mpiexec -np $(($max_number_of_sieves+1)) dispel4py mpi dispel4py.measure.graph.repeatable_prime_sieve__static_$max_number_of_sieves -i $number_of_iteration &&
	/usr/bin/time -o $fn_t_mpi -f %e mpiexec -np $(($max_number_of_sieves+1)) dispel4py mpi dispel4py.measure.graph.repeatable_prime_sieve__static_$max_number_of_sieves -i $number_of_iteration > stdout_mpi 2> stderr_mpi &&

	mpi_inc_time=`cat $fn_t_mpi_inc | tr -d '\n'` &&
	mpi_time=`cat $fn_t_mpi | tr -d '\n'` &&

	echo $platform $version $run_id $number_of_iteration $np $max_number_of_sieves $max_prime $mpi_time $mpi_inc_time | tee -a "$overall_file"
}

pair_1="26 100"
pair_2="168 1000"
pair_3="1397 10000"
all=(
pair_1
pair_2
pair_3
)

np=3

echo Ready

length=${#all[@]}
for ((i=0;i<$length;i++)) do
	pair=${!all[i]}
	for ((number_of_iteration=1;number_of_iteration<100;number_of_iteration+=10)); do
		for ((iter=0;iter<8;iter+=1)); do step $np $number_of_iteration $pair; done
	done
done

deactivate

