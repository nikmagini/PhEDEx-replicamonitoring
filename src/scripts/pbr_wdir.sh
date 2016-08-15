#!/bin/sh
# Author: Aurimas Repecka <aurimas.repecka AT gmail [DOT] com>
# Based On Work By: Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
# A wrapper script to submit spark job with pbr.py script

# test arguments
if [ "$#" -eq 0 ]; then
    echo "Usage: pbr <options>"
    echo "       pbr --help"
    exit 1
fi

wdir=$PWD/../python

if [ "$1" == "-h" ] || [ "$1" == "--help" ] || [ "$1" == "-help" ]; then
    # run help
    python $wdir/pbr.py --help
elif [[  $1 =~ -?-yarn(-cluster)?$ ]]; then
    # to tune up these numbers:
    #  - executor-memory not more than 5G
    #  - num-executor can be increased (suggested not more than 10)
    #  - cores = 2/4/8
    # Temp solution to have a wrapper for python27 on spark cluster
    # once CERN IT will resolve python version we can remove PYSPARK_PYTHON
#    PYSPARK_PYTHON='/afs/cern.ch/user/v/valya/public/python27'
    PYSPARK_PYTHON='/etc/spark/python' \
    spark-submit \
	--master yarn-client \
	--executor-memory 5g \
	--packages com.databricks:spark-csv_2.10:1.4.0 \
	$wdir/pbr.py ${1+"$@"}
else
    PYSPARK_PYTHON='/afs/cern.ch/user/v/valya/public/python27'
    spark-submit \
	--executor-memory $((`nproc`/4))G \
        --master local[$((`nproc`/4))] \
	--packages com.databricks:spark-csv_2.10:1.4.0 \
	$wdir/pbr.py ${1+"$@"}
fi
