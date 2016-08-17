# User guide 

## Parameters

### fname

Fname is used fo specifying one input data file in hdfs. If not specified assumption is made that user uses script in multi-file mode, so the parameter basedir expected to be defined. If none of these parameters (fname, basedir) are defined user gets an error.

### fout

Fout is used for specifying output file name. If the file already exists user gets an error. If not specified, data is not written to the disk - first 15 results are displayed to user.

### basedir

Basedir is used for specifying directory in which hdfs files are held. Files in the directory suppose to have date (YYYY-mm-dd) string in their names. Parameter is used along with parameters fromdate and todate to filter files that are going to be processed. If either: fromdate or todate is not specified default value: now() is set for both of these variables, so only today snapshot is being processed.

### fromdate

Fromdate is used for specifying date from which hdfs files in basedir will be processed. Date format is expected to be YYYY-mm-dd. If not - user gets an error. If not specified default value: now() is set.

### todate

Tdate is used for specifying date until which hdfs files in basedir will be processed. Date format is expected to be YYYY-mm-dd. If not - user gets an error. If not specified default value: now() is set.

### keys

Keys are used for specifying key fields for group operation. Keys are expected to be written in csv manner. If not - user gets an error. If parameter is not set - default value is set (dataset_name, node_name). Possible values for keys:

```
now_sec, now, dataset_name, block_name, node_name, br_is_custiodial, br_user_group, data_tier, acquisition_era, node_kind
```

### results

Results are used for specifying result fields for group operation. Results are expected to be written in csv manner. If not - user gets an error. If parameter is not set - default value is set (block_files, block_bytes). Possible values for results:

```
block_files, block_bytes, br_src_files, br_src_bytes, br_dest_files, br_dest_bytes, br_node_files, br_node_bytes, br_xfer_files, br_xfer_bytes
```

### aggregations

Aggregations are used for specifying aggregation functions for group operation. If the same aggregation function should be used fo all results columns then it is enough to specify one aggregation function. If user wants to specify different aggregation functions for different columns then aggregations is expected to be written in csv manner and in the exact order as results were specified. If parameter is not set - default value is set (sum) for all results elements. Possible values for aggregations:

```
sum, count, min, max, first, last, mean
```

### order

Order is used for specifying sort order for aggregated data. Order is expected to be written in csv manner and should contain fields only from keys and results parameters. If not - user gets an error. This parameter goes along with parameter asc. If parameter is not set - data will not be sorted. 

### asc

Asc is used for specifying sort order for order columns. Asc is expected to be written in csv manner and should contain only 1,0 (1 - ascending, 0 -descending). Symbols 1,0 should appear in the exact same order as columns in order parameter. This parameter goes along with parameter ord. If parameter is not set - all columns will be sorted ascending. 

### header

Header is used for writing column names of aggregation columns in the frist line of destination csv file. It should used only with fout parameter, otherwise it has no effect.

### verbose

Verbose is used for printing dataframes schema during data processing.

### yarn

Yarn is used for activitacing Yarn cluster management technology.

### interval

Interval is used for delta operation. It defines between what periods delta will be calculated. Interval represents duration in days, so it should be defined as integer number.

### filt

Filt is used for data filtering on one field from group keys. Filter is expected to be written in form - field:value. Field must match element from group keys list.

```
bash pbr.sh --yarn \
		--basedir hdfs:///project/awg/cms/phedex/block-replicas-snapshots/csv/ \
		--fromdate 2015-08-04 \
		--todate 2015-08-09 \
		--keys data_tier,acquisition_era \
		--results br_node_files,br_dest_files \
		--aggregations min,sum \
		--order data_tier,br_dest_files \
		--asc 0,1
		#--interval 1 
        #--filt node_name:T2_US_Florida
		#--header
		#--fout hdfs:///user/arepecka/ReplicaMonitoring
		#--verbose 
		#--fname hdfs:///project/awg/cms/phedex/block-replicas-snapshots/csv/time=2016-07-09_03h07m28s 


# An example result of such a query could be as follows:

# Between dates 2016-08-04 and 2016-08-09 found 7 files
#--------------------------------------------------------------------------
#-- data_tier - acquisition_era - min(br_node_files) - sum(br_dest_files)--
#--------------------------------------------------------------------------
#-- RECO      - patTest         - 0                  - 6
#-- RECO      - CMSSW_1_8_4     - 0                  - 84
#-- RAW       - CMSSW_1_6_7     - 0                  - 12
#-- RAW       - null            - 0                  - 48
#--------------------------------------------------------------------------
```

