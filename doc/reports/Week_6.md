# Weekly Reports 

## Week 6

- Tested preformance on delta operation.
	- Result: br_node_bytes
	- Interval: 1

| Interval  | Input  | Containers | Cores | Memory  | Output  | Duration |
|-----------|--------|------------|-------|---------|---------|----------|
| 2 days    | ~5GB   | 65         | 65    | 361472MB| 12.7KB  | ~2.5min  |
| 7 days    | ~20GB  | 65         | 65    | 361472MB| 41.6KB  | ~4min    |
| 1 month   | ~90GB  | 65         | 65    | 361472MB| 189.2KB | ~8.5min  |
| 6 months  | ~500GB | 65         | 65    | 361472MB| 1MB     | ~35min   |

- Fixed memory issues for delta operation on 1 month or more data
    - Increased executors memory to 5GB
    - Algorithm restructuring

Nevertheless, I did not manage to run the script on all of data: ~900GB. Because large amounts of data appear in shuffle operations, executors memory (increased to 5G    B) is not enough. That causes spills and an error occurs.
- Restructured delta operation for better performance
```
#2 group data by block, node, interval and last result in the interval
ndf = ndf.select(ndf.block_name, ndf.node_name, ndf.now, getattr(ndf, result))
idf = ndf.withColumn("interval_group", interval_group(ndf.now))
win = Window.partitionBy(idf.block_name, idf.node_name, idf.interval_group).orderBy(idf.now.desc())
idf = idf.withColumn("row_number", rowNumber().over(win))
rdf = idf.where((idf.row_number == 1) & (idf.interval_group != 0))\
          .withColumn(result, when(idf.now == interval_end(idf.interval_group), getattr(idf, result)).otherwise(lit(0)))
rdf = rdf.select(rdf.block_name, rdf.node_name, rdf.interval_group, getattr(rdf, result))
rdf.cache()

#3 create intervals that not exist but has minus delta
win = Window.partitionBy(idf.block_name, idf.node_name).orderBy(idf.interval_group)
adf = rdf.withColumn("interval_group_aft", lead(rdf.interval_group, 1, 0).over(win))
hdf = adf.filter(((adf.interval_group + 1) != adf.interval_group_aft) & (adf.interval_group != max_interval))\
         .withColumn("interval_group", adf.interval_group + 1)\
         .withColumn(result, lit(0))\
         .drop(adf.interval_group_aft)

#4 join data frames
idf = rdf.unionAll(hdf)
       
#3 join every interval with previous interval
win = Window.partitionBy(idf.block_name, idf.node_name).orderBy(idf.interval_group)
fdf = idf.withColumn("delta", getattr(idf, result) - lag(getattr(idf, result), 1, 0).over(win))

#5 calculate delta_plus and delta_minus columns and aggregate by date and node
ddf =fdf.withColumn("delta_plus", when(fdf.delta > 0, fdf.delta).otherwise(0)) \
        .withColumn("delta_minus", when(fdf.delta < 0, fdf.delta).otherwise(0))
         
aggres = ddf.groupBy(ddf.node_name, ddf.interval_group).agg(sum(ddf.delta_plus).alias("delta_plus"),\
                                                             sum(ddf.delta_minus).alias("delta_minus"))
                                                             
aggres = aggres.select(aggres.node_name, interval_end(aggres.interval_group).alias("date"), aggres.delta_plus, aggres.delta_minus)
```
- Preparation work for RPM:
    - Changed folder structure
    - Tagged git repository
- Understanding node.js for data vizualization
