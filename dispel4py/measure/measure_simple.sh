#!/bin/sh

trap "exit;" SIGINT

platform=$1

measure_dir=$PWD/measure
mkdir -p "$measure_dir"

overall_file="$measure_dir/overall_simple"
if [ ! -f "$overall_file" ]; then
	echo platform number_of_iteration np mpi_time mpi_inc_time
	echo platform number_of_iteration np mpi_time mpi_inc_time > "$overall_file"
fi

source ~/self/Edinburgh/venv/dissertation/bin/activate

function step {
	np=$1
	number_of_iteration=$2

	echo np:$np number_of_iteration:$number_of_iteration

	wd=$measure_dir/`date` &&
	mkdir -p "$wd" &&
	cd "$wd" &&
	mkdir -p outputs/mpi{,_inc} &&

	echo Number of initial nodes: $np > configure &&
	echo Number of iterations: $number_of_iteration >> configure &&

	/usr/bin/time -f %e mpiexec -np $np dispel4py mpi dispel4py.measure.graph.large_teecopy -i $number_of_iteration > /dev/null 2> time_mpi &&
	echo "=================" &&
	/usr/bin/time -f %e mpiexec -np $np dispel4py mpi_inc dispel4py.measure.graph.large_teecopy -i $number_of_iteration > /dev/null 2> time_mpi_inc &&

	#mpi_inc_time=`cat mpi_inc | tr -d '\n'` &&
	#mpi_time=`cat mpi | tr -d '\n'` &&
	mpi_inc_time=`cat time_mpi_inc | tr -d '\n'` &&
	mpi_time=`cat time_mpi | tr -d '\n'` &&

	echo $platform $number_of_iteration $np $mpi_time $mpi_inc_time
	echo $platform $number_of_iteration $np $mpi_time $mpi_inc_time >> "$overall_file"
}

echo Ready

for ((np=4;np<=8;np++)) do
	for ((number_of_iteration=1;number_of_iteration<1000;number_of_iteration+=10)); do
		for ((iter=0;iter<5;iter+=1)); do step $np $number_of_iteration; done
	done
done

deactivate

