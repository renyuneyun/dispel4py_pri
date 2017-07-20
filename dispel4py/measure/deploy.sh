#!/bin/sh

cd ../..
d4p_path=`pwd`

cd $HOME

venv_path=$HOME/venv

virtualenv "$venv_path"
source "$venv_path"/bin/activate

pip install dispel4py
pip install futures

git clone https://bitbucket.org/mpi4py/mpi4py.git
cd mpi4py
python setup.py install

cd $venv_path/lib/python*/site-packages
mv dispel4py dispel4py.ori
ln -s $d4p_path/dispel4py

deactivate

