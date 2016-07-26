bash pbr.sh \
		--basedir hdfs:///project/awg/cms/phedex/block-replicas-snapshots/csv/ \
		--fromdate 2015-08-04 \
		--todate 2015-08-09 \
		--keys data_tier,acquisition_era \
		--results br_node_files,br_dest_files \
		--aggregations min,sum \
		--order data_tier,br_dest_files \
		--asc 0,1 \
		--yarn
		#--fout hdfs:///user/arepecka/ReplicaMonitoring
		#--verbose 
		#--fname hdfs:///project/awg/cms/phedex/block-replicas-snapshots/csv/time=2016-07-09_03h07m28s



