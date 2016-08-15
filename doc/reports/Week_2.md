# Weekly Reports 

## Week 2

### Dynamic management
Added dynamic management in aggregation process. The idea of dynamic management is to follow two steps rule:
- Map function applied to dataframe should create key-value tuple by given input (group keys and group results)
- ReduceByKey function applied to dataframe should aggregate key-value tuple by given function

So the basic form of this should look like:
```
ndf.map(mapf).reduceByKey(reducef)
# Example of calculating sum by give group keys and results
ndf.map(lambda r: ((r.now, r.dataset_name),(r.node_files, r.node_bytes))).reduceByKey(lambda x, y : map(sum, zip(x, y))) 
```

Class LambdaBuilder is responsible for creating dynamic lambdas for map and reduce functions. The main principle is to use given user input lists and getattr function for dynamic management.
```
# dynamic lambda for map function (for creating key-value tuple). User input: keys, res.
return lambda r: (tuple([getattr(r, k) for k in keys]), tuple([getattr(r, k) for k in res]))

# dynamic call for aggregation function. User input: lambdaf
return getattr(self, lambdaf)()

#dynamic lambda for sum aggregation function. Zip and map is used because we do not know dimensions of given x and y tuples
return lambda x, y : map(sum, zip(x, y))
```

#### Dynamic group keys
Added possibility to specify group keys dynamically in csv manner (ex. --keys now,dataset_name,br_user_group,node_kind).
For now supported group keys are:
```
now, dataset_name, block_name, node_name, br_is_custiodial, br_user_group, data_tier, acquisition_era, node_kind
```

#### Dynamic group results
Added possibility to specify group results dynamically in csv manner (ex. --results br_node_files,br_dest_files,br_src_files).
For now supported group results are:
```
block_files, block_bytes, br_src_files, br_src_bytes, br_dest_files, br_dest_bytes, br_node_files, br_node_bytes, br_xfer_files, br_xfer_bytes
```

#### Dynamic group functions
Added possibility to specify aggregation function dynamically (ex. --lambdaf sumf). For now it is posiible to calculate only sum function. In future perspective it should be possible to specifiy other aggregation functions like mean, max, min, etc. It should also be possible to specify different aggregation function for every column.

### Filtering empty values
The program itself by default shows only those aggregation results which have no null values in their keys (ex. 1467683123.000000000000000000000000000003,/GlobalAug07-B/Online/RAW,DataOps,MSS,null will not be shown). This was done considering that these results are not valid. On the other hand, --empty parameter was added to show results that has null values in their keys.
```
ndf.map(mapf).filter(lambda v: not isEmpty(v[0])).reduceByKey(reducef)
```

### Filtering by date
Added possibility to process several phedex snapshots by date. In that case user needs to specify base directory parameter (--basedir), from date (--fromdate) and to date (--todate). An example of calling script by using filtering by date:
```
bash ./pbr.sh --keys br_user_group,node_kind,data_tier --results br_node_files --basedir hdfs:///project/awg/cms/phedex/block-replicas-snapshots/csv/ --fromdate 2015-08-01 --todate 2015-08-10
```
The date expected to be in YYYY-mm-dd format. The folders in base directory should also have that format date in its names (ex. time=2015-08-01_03h08m25s). Later filtered files are loaded using union function (in the future this might be changed as it could cause performance issues)
```
sc.union([sc.textFile(file_path).map(lambda line: line.split(",")) for file_path in files])
```
 
### Parsing additional data
Added additional data needed to be parsed:
- Acquisition era and data tier. These elements are parsed from dataset name using regex. Assumption was made that all dataset names are written in form: /PrimaryDataset/AcquisitionEra-ProcessingEra/DataTier. Although, testing on data showed that there are many records that do not satisfy this assumption. In that case these elements are set to null and aggregated results are only visible when parameter --empty is set.
- Group name and node kind. These elements are needed for joins. It was considered that these tables are not likely to change a lot, so the local file approach was chosen. Two csv files are stored locally in the code_directory/additional_data (phedex_groups.csv, phedex_node_kinds.csv). The data needed for joins is read from them and stored in dictionaries. Better approach might be chosen if data will be considered as often changing or memory consuming.

### Efficiency tesing
One daily snapshot is now processed in ~1 minute. 7 snapshots are processsed in ~7 minutes. These results were collected while submitting spark job using "--master local". When submitting a job with "--master yarn-client" the results were approximately the same, so assumption was made that maybe task parallelism is not working properly. Further investigation is needed.


