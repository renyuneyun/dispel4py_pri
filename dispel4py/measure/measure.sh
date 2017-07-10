#!/bin/sh

trap "exit;" SIGINT

platform=$1

measure_dir=$PWD/measure
mkdir -p "$measure_dir"

overall_file="$measure_dir/overall"
if [ ! -f "$overall_file" ]; then
	echo platform number_of_iteration np max_number_of_sieves max_prime mpi_time mpi_inc_time
	echo platform number_of_iteration np max_number_of_sieves max_prime mpi_time mpi_inc_time > "$overall_file"
fi

source ~/self/Edinburgh/venv/dissertation/bin/activate

function step {
	np=$1
	number_of_iteration=$2

	max_number_of_sieves=$3
	max_prime=$4

	echo np:$np number_of_iteration:$number_of_iteration max_number_of_sieves:$max_number_of_sieves max_prime:$max_prime

	wd=$measure_dir/`date` &&
	mkdir -p "$wd" &&
	cd "$wd" &&
	mkdir -p outputs/mpi{,_inc} &&

	echo Number of initial nodes: $np > configure &&
	echo Number of iterations: $number_of_iteration >> configure &&

	/usr/bin/time -f %e mpiexec -np $np dispel4py mpi_inc dispel4py.measure.graph.repeatable_prime_sieve -i $number_of_iteration > /dev/null 2> time_mpi_inc &&
	echo "=================" &&
	/usr/bin/time -f %e mpiexec -np $(($max_number_of_sieves+1)) dispel4py mpi dispel4py.measure.graph.repeatable_prime_sieve__static_$max_number_of_sieves -i $number_of_iteration > /dev/null 2> time_mpi &&

	#mpi_inc_time=`cat mpi_inc | tr -d '\n'` &&
	#mpi_time=`cat mpi | tr -d '\n'` &&
	mpi_inc_time=`cat time_mpi_inc | tr -d '\n'` &&
	mpi_time=`cat time_mpi | tr -d '\n'` &&

	echo $platform $number_of_iteration $np $max_number_of_sieves $max_prime $mpi_time $mpi_inc_time
	echo $platform $number_of_iteration $np $max_number_of_sieves $max_prime $mpi_time $mpi_inc_time >> "$overall_file"
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

