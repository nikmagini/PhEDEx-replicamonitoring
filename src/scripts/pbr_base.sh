#!/bin/sh
# Author: Aurimas Repecka <aurimas.repecka AT gmail [DOT] com>
# A wrapper script to submit spark job with pbr.sh script

bash pbr.sh --yarn \
            --basedir hdfs:///project/awg/cms/phedex/block-replicas-snapshots/csv/ \
            --fromdate 2016-07-01 \
            --todate 2016-07-07 \
            --results br_node_bytes \
            --aggregations delta \
            --interval 1 \
            --fout hdfs:///cms/phedex-monitoring/output
            #--filt node_name:T2_US_Florida \
            #--keys now \
            #--order now,node_name \
            #--asc 0,1 \
            #--header \
            #--verbose \
            #--fname /home/aurimas/CERN/ReplicaMonitoring/v2/data/project/awg/cms/phedex/block-replicas-snapshots/csv/time=2016-07-09_03h07m28s 




