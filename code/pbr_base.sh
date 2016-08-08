#!/bin/sh
# Author: Aurimas Repecka <aurimas.repecka AT gmail [DOT] com>
# A wrapper script to submit spark job with pbr.sh script

bash pbr.sh --yarn \
                --basedir hdfs:///project/awg/cms/phedex/block-replicas-snapshots/csv/ \
                --fromdate 2016-07-01 \
                --todate 2016-07-07 \
                --keys br_user_group,data_tier,acquisition_era,node_kind \
                --results br_node_bytes \
                --aggregations delta \
                --interval 1 \
                --fout hdfs:///user/arepecka/ReplicaMonitoring
                #--verbose 
                #--fname /home/aurimas/CERN/ReplicaMonitoring/v2/data/project/awg/cms/phedex/block-replicas-snapshots/csv/time=2016-07-09_03h07m28s 

