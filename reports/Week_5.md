# Weekly Reports 

## Week 5

- Tested preformance on basic operations.
	- Group keys: node_kind,br_user_group,data_tier,acquisition_era
	- Group results: br_node_bytes
	- Aggregation: sum

| Interval  | Input  | Containers | Cores | Memory  | Output  | Duration |
|-----------|--------|------------|-------|---------|---------|----------|
| 1 day     | ~2GB   | 27         | 27    | 40960   | 15.8KB  | ~2min    |
| 1 month   | ~90GB  | 65         | 65    | 99328MB | 493.3KB | ~5min    |
| 3 months  | ~200GB | 65         | 65    | 99328MB | 519.3KB | ~11min   |
| 12 months | ~850GB | 65         | 65    | 99328MB | 632.2KB | ~30min   |

- Forming unique file header for every output (Format: YYYY-mm-dd_HHhMMmSSs_execution_N)
```
def formFileHeader(fout):
	now_date = dt.strftime(dt.now(), "%Y-%m-%d")
	file_list = os.popen("hadoop fs -ls %s" % fout).read().splitlines()
	# if files are not in hdfs --> file_list = os.listdir(fout)
	now_file_list = [file_path for file_path in file_list if now_date in file_path]
	return  fout + "/" + dt.strftime(dt.now(), "%Y-%m-%d_%H:%M:%S") + "_execution_" + \
				str(len(now_file_list)+1)
```
- Solved disk quota problems by submitting a ticket for disk space increasing up to 300GB
- Developed delta function for calculating block transfers between different time intervals. The operation is not dynamic. It has fixed fields: node_name, date. User can only specify one result field (most often it will be br_node_bytes) and interval in days. The basic algorithm:
	- For all dates in the given period generate interval group
	```
	def generateDateDict(fromdate_str, todate_str, interval_str):
		fromdate = dt.strptime(fromdate_str, "%Y-%m-%d")
		todate = dt.strptime(todate_str, "%Y-%m-%d")
		interval = int(interval_str)

		currentdate = fromdate
		currentgroup = 1
		elementcount = 0
		dategroup_dic = {}

		while currentdate <= todate:
			dategroup_dic[dt.strftime(currentdate, "%Y-%m-%d")] = currentgroup
			currentdate = currentdate + timedelta(days=1)
			elementcount += 1
			if elementcount >= interval:
				elementcount = 0
				currentgroup += 1 	

		return dategroup_dic
	```
	- Group data by block, node, interval and retrieve the newest result value in the interval. If newest value in the interval does not match the end in the interval - newest result value 0.
	```	
	win = Window.partitionBy(idf.block_name, idf.node_name, idf.interval_group).orderBy(idf.now.desc())		
	idf = idf.withColumn("row_number", rowNumber().over(win))
	rdf = idf.where(idf.row_number == 1).withColumn(result, when(idf.now == interval_end(idf.interval_group), getattr(idf, result)).otherwise(lit(0)))
	```
	- Generate records for blocks that disappeared from node (in csv there is no row because block is missing but the period has minus delta)
	```
	adf = adf.withColumn("interval_group", adf.interval_group  - 1)
	cond = [rdf.interval_group == adf.interval_group, rdf.block_name == adf.block_name, rdf.node_name == adf.node_name]
	mdf = rdf.join(adf, cond, "leftsemi")
	hdf = rdf.subtract(mdf).filter(rdf.interval_group != max_interval).select(rdf.block_name, rdf.node_name, \
										 (rdf.interval_group + 1).alias("interval_group"), lit(0))
	```
	- Join existing and generated records
	```
	idf = rdf.unionAll(hdf)
	```
	- Calculate deltas between intervals
	```
	win = Window.partitionBy(idf.block_name, idf.node_name).orderBy(idf.interval_group)		
	fdf = idf.withColumn("delta", getattr(idf, result) - lag(getattr(idf, result), 1, 0).over(win))	
	```
	- Dvide delta_plus and delta_minus columns and aggregate by date and node
	```
	ddf =fdf.withColumn("delta_plus", when(fdf.delta > 0, fdf.delta).otherwise(0)) \
			.withColumn("delta_minus", when(fdf.delta < 0, fdf.delta).otherwise(0))

	aggres = ddf.groupBy(ddf.node_name, ddf.interval_group).agg(sum(ddf.delta_plus).alias("delta_plus"),\
												 				sum(ddf.delta_minus).alias("delta_minus"))
	```
- Tested preformance on delta operations.
	- Group results: br_node_bytes
	- Aggregation: delta

| Records   | Input  | Containers | Cores | Memory  | Output  | Duration |
|-----------|--------|------------|-------|---------|---------|----------|
| 2 days    | ~2GB   | 27         | 27    | 40960   | 12.7KB  | ~2.5min  |
| 7 days   	| ~20GB  | 65         | 65    | 99328MB | 41.6KB  | ~5min    |
| 1 month  	| ~200GB | -          | -     | -       | -       | -        |
| 1 year    | ~850GB | -          | -     | -       | -       | -        |

> />=30 records were not processed due to errors 
