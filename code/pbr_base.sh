bash pbr.sh --yarn \
		--basedir hdfs:///project/awg/cms/phedex/block-replicas-snapshots/csv/ \
		--fromdate 2015-08-04 \
		--todate 2015-09-04 \
		--keys data_tier,acquisition_era \
		--results br_node_files,br_dest_files \
		--aggregations sum,sum \
		--order data_tier,br_dest_files \
		--asc 0,1  
		#--header 
		#--fout hdfs:///user/arepecka/ReplicaMonitoring
		#--verbose 
		#--fname /home/aurimas/CERN/ReplicaMonitoring/v2/data/project/awg/cms/phedex/block-replicas-snapshots/csv/time=2016-07-09_03h07m28s 

