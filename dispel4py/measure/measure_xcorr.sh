#!/bin/sh

trap "exit;" SIGINT

if [ ! -z $1 ]; then
	platform=$1
else
	platform=unspecified_platform
fi
version=opt1.1_
run_id=`date +%Y-%m-%d.%H:%M:%S`

source ~/self/Edinburgh/venv/dissertation/bin/activate
xcorr_dir="$HOME/self/Edinburgh/MSc Dissertation/dispel4py_workflows/tc_cross_correlation"
export PYTHONPATH=$PYTHONPATH:"$xcorr_dir"

measure_dir=$PWD/measure_xcorr
mkdir -p "$measure_dir"

overall_file="$measure_dir/overall"
if [ ! -f "$overall_file" ]; then
	echo workflow platform version run_id module number_of_iteration np max_number_of_sieves max_prime time | tee "$overall_file"
fi

export DISPEL4PY_XCORR_STARTTIME=2015-04-06T06:00:00.000
export DISPEL4PY_XCORR_ENDTIME=2015-04-06T07:00:00.000

function step {
	np=$1

	fn_t_mpi_prep=time_mpi_prep
	fn_t_mpi_xcorr=time_mpi_xcorr
	fn_t_mpi_inc_prep=time_mpi_inc_prep
	fn_t_mpi_inc_xcorr=time_mpi_inc_xcorr

	wf_prep=realtime_prep
	wf_xcorr=realtime_xcorr
	wf_use_prep="$xcorr_dir/$wf_prep.py"
	wf_use_xcorr="$xcorr_dir/$wf_xcorr.py"
	cfg="$xcorr_dir/realtime_xcorr_input.jsn"

	wf_mpi_prep=$wf_prep
	wf_use_mpi_prep=$wf_use_prep
	wf_mpi_xcorr=$wf_xcorr
	wf_use_mpi_xcorr=$wf_use_xcorr

	wf_mpi_inc_prep=$wf_prep
	wf_use_mpi_inc_prep=$wf_use_prep
	wf_mpi_inc_xcorr=$wf_xcorr
	wf_use_mpi_inc_xcorr=$wf_use_xcorr

	np_mpi_prep=$np
	np_mpi_xcorr=$np
	np_mpi_inc_prep=$((np+1))
	np_mpi_inc_xcorr=$((np+1))

	exec_mpi_prep="/usr/bin/time -o $fn_t_mpi_prep -f %e mpiexec -np $np_mpi_prep dispel4py mpi \"$wf_use_mpi_prep\" -f \"$cfg\""
	exec_mpi_xcorr="/usr/bin/time -o $fn_t_mpi_xcorr -f %e mpiexec -np $np_mpi_xcorr dispel4py mpi \"$wf_use_mpi_xcorr\""
	exec_mpi_inc_prep="/usr/bin/time -o $fn_t_mpi_inc_prep -f %e mpiexec -np $np_mpi_inc_prep dispel4py mpi_inc \"$wf_use_mpi_inc_prep\" -f \"$cfg\""
	exec_mpi_inc_xcorr="/usr/bin/time -o $fn_t_mpi_inc_xcorr -f %e mpiexec -np $np_mpi_inc_xcorr dispel4py mpi_inc \"$wf_use_mpi_inc_xcorr\""

	wd=$measure_dir/`date +%Y-%m-%d.%H:%M:%S` &&
	echo log dir: $wd &&
	mkdir -p "$wd" &&
	cd "$wd" &&
	mkdir -p outputs/mpi{,_inc} &&

	xcorr_gen_dir=./tc_cross_correlation/OUTPUT &&
	mkdir -p $xcorr_gen_dir &&

	echo Number of initial nodes: $np > configure &&
	echo Number of iterations: $number_of_iteration >> configure &&

	rm -rf "$xcorr_gen_dir/DATA" &&
	rm -rf "$xcorr_gen_dir/XCORR" &&
	mkdir "$xcorr_gen_dir/DATA" &&
	mkdir "$xcorr_gen_dir/XCORR" &&

	echo $exec_mpi_inc_prep &&
	eval $exec_mpi_inc_prep > stdout_mpi_inc_prep 2> stderr_mpi_inc_prep &&
	echo $exec_mpi_inc_xcorr &&
	eval $exec_mpi_inc_xcorr > stdout_mpi_inc_xcorr 2> stderr_mpi_inc_xcorr &&

	rm -rf "$xcorr_gen_dir/DATA" &&
	rm -rf "$xcorr_gen_dir/XCORR" &&
	mkdir "$xcorr_gen_dir/DATA" &&
	mkdir "$xcorr_gen_dir/XCORR" &&

	echo $exec_mpi_prep &&
	eval $exec_mpi_prep > stdout_mpi_prep 2> stderr_mpi_prep &&
	echo $exec_mpi_xcorr &&
	eval $exec_mpi_xcorr > stdout_mpi_xcorr 2> stderr_mpi_xcorr &&

	mpi_time_prep=`cat $fn_t_mpi_prep | tr -d '\n'` &&
	mpi_time_xcorr=`cat $fn_t_mpi_xcorr | tr -d '\n'` &&
	mpi_inc_time_prep=`cat $fn_t_mpi_inc_prep | tr -d '\n'` &&
	mpi_inc_time_xcorr=`cat $fn_t_mpi_inc_xcorr | tr -d '\n'` &&

	echo $wf_mpi_prep $platform $version $run_id mpi $number_of_iteration $np_mpi_prep $max_number_of_sieves $max_prime $mpi_time_prep | tee -a "$overall_file" &&
	echo $wf_mpi_xcorr $platform $version $run_id mpi $number_of_iteration $np_mpi_xcorr $max_number_of_sieves $max_prime $mpi_time_xcorr | tee -a "$overall_file" &&
	echo $wf_mpi_inc_prep $platform $version $run_id mpi_inc $number_of_iteration $np_mpi_inc_prep $max_number_of_sieves $max_prime $mpi_inc_time_prep | tee -a "$overall_file" &&
	echo $wf_mpi_inc_xcorr $platform $version $run_id mpi_inc $number_of_iteration $np_mpi_inc_xcorr $max_number_of_sieves $max_prime $mpi_inc_time_xcorr | tee -a "$overall_file"
}

all=(
lol
)

np=4

echo Ready

length=${#all[@]}
for ((i=0;i<$length;i++)) do
	pair=${!all[i]}
	for ((iter=0;iter<8;iter+=1)); do
		step $np
	done
done

deactivate

