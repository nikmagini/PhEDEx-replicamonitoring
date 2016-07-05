# Dataset replica monitoring
Monitoring the evolution of storage space occupation by different types of CMS datasets

## Project description
CMS data are recorded in files, which are organized in datasets and blocks.
Datasets are set of files with common physics content, and have a size ranging from 
a few files and few GBs to several hundred thousand files and hundred TBs depending on the 
physics definition. Datasets are divided into groups of files called blocks to simplify data management;
each block has a typical size of 100-1000 files and 100 GB-1 TB.
The CMS data management system PhEDEx distributes the data to tens of sites and tracks in its Oracle database
the location of every replica of every block produced by CMS.
We propose to extend PhEDEx monitoring with a system to generate statistics about the storage space occupied by different types of datasets at different sites.
Since the PhEDEx database only contains the current status of the block replicas, to preserve historical evolution of space occupation we export daily snapshots
of the PhEDEx block replica information to HDFS.
Your task is to set up a service (potentially Zeppelin notepad+Spark, or pySpark) to perform the daily aggregation of block replica snapshots on HDFS at different levels.
Depending on the performance, the aggregation can run as a daily cron job storing the results, or it can be a live query.
The next step would be to set up a system to visualize both the current the time evolution of the results .

Tools to review:
- python language
- HDFS, spark

### PhEDEx block replica data
PhEDEx block replica snapshots can be found on HDFS: data can be found on HDFS:/project/awg/cms/phedex/block-replicas-snapshots/csv
```
   hdfs dfs -ls /project/awg/cms/phedex/block-replicas-snapshots/csv
```
The schema description is here:
http://awg-virtual.cern.ch/data-sources-index/projects/#phedex-blk-replicas-snapshot
and in short it is:

```
now, dataset_name, dataset_id, dataset_is_open, dataset_time_create, dataset_time_update,
block_name, block_id, block_files, block_bytes, block_is_open, block_time_create, block_time_update,
node_name, node_id, br.is_active, br.src_files, br.src_bytes, br.dest_files, br.dest_bytes,
br.node_files, br.node_bytes, br.xfer_files, br.xfer_bytes, br.is_custodial, br.user_group, replica_time_create, replica_time_update
```

Here is an example of rows from HDFS
```
1466560216.000000000000000000000000000002,/LogMonitor/Run2012D-v1/RAW,557315,y,1348765642.50834,null,/LogMonitor/Run2012D-v1/RAW#5810fa64-2ff7-11e2-9d5d-842b2b4671d8,3970074,1,862222,n,1353075127.37014,1353076085.29089,T0_CH_CERN_MSS,2,n,0,0,1,862222,1,862222,0,0,y,18,1353075127,1353336461.59835
```

An example csv file with 10k entries is here:

https://github.com/nikmagini/PhEDEx-replicamonitoring/blob/master/data/2016-07-05-block-replicas-sample

### Queries of interest

CMS Dataset names follow this convention:

/PrimaryDataset/AcquisitionEra-ProcessingEra/DataTier

https://twiki.cern.ch/twiki/bin/view/CMS/DMWMPG_PrimaryDatasets

We are interested in querying the total size occupied (sum of all block replica bytes) grouped by user group, datatier, acquistionera, and node kind.
An example query to perform this against the Oracle PhEDEx DB is found here:
https://github.com/dmwm/PHEDEX/blob/master/Contrib/datavolume_by_group_node_era_tier.sql

You may reimplement this against HDFS adding the time evolution.
You may then extend the queries to different/more generic groupings.

### Code examples

Example code to perform queries against block replica data on HDFS:

https://github.com/vkuznet/CMSSparkExamples/tree/master/phedex
