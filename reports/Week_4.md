# Weekly Reports 

## Week 4

- Aggregations can be defined to every result column seperately. If the same aggregation function should be used fo all results columns then it is enough to specify one aggregation function. If user wants to specify different aggregation functions for different columns then aggregations is expected to be written in csv manner and in the exact order as results were specified. If parameter is not set - default value is set (sum) for all results elements.
- The set of aggregation functions was espanded to:
```
sum, count, min, max, first, last, mean
```
- Added possibility for data ardering (parameter --order, --asc). 
	- Order is expected to be written in csv manner and should contain fields only from keys and results parameters. If not - user gets an error. This parameter goes along with parameter asc. If parameter is not set - data will not be sorted.
	- Asc is expected to be written in csv manner and should contain only 1,0 (1 - ascending, 0 -descending). Symbols 1,0 should appear in the exact same order as columns in order parameter. This parameter goes along with parameter ord. If parameter is not set - all columns will be sorted ascending.
- For csv file reading and writing started using spark-csv external package. It also speeded up a script as there was no need to use python split for splitting data into columns and converting columns into string.
```
pdf = unionAll([sqlContext.read.format('com.databricks.spark.csv').options(treatEmptyValuesAsNulls='true', nullValue='null')load(file_path, schema = schema_def) for file_path in files])
pdf.write.format('com.databricks.spark.csv').save(opts.fout)
```
- When saving dataframe in hdfs parameter --header can be set. Header is used for writing column names of aggregation columns in the frist line of destination csv file. It should used only with fout parameter, otherwise it has no effect.
```
aggres.write.format('com.databricks.spark.csv').options(header = 'true').save(opts.fout)
```
- Spark sql schema was defined using spark StructType (column name, type, nullable). Now there is no need for additional data casting.
- Changed "br_user_group, node_kind, now, acquisition_era, data_tier" fields parsing. Using spark sql functions and udfs in order to get better performance. 
```
acquisition_era_reg = r"^/[^/]*/([^/^-]*)-[^/]*/[^/]*$"	
data_tier_reg = r"^/[^/]*/[^/^-]*-[^/]*/([^/]*)$"
groupf = udf(lambda x: groupdic[x], StringType())
nodef = udf(lambda x: nodedic[x], StringType())
acquisitionf = udf(lambda x: regexp_extract(x, acquisition_era_reg, 1) or "null")
datatierf = udf(lambda x: regexp_extract(x, data_tier_reg, 1) or "null")

ndf = pdf.withColumn("br_user_group", groupf(pdf.br_user_group_id)) \
		 .withColumn("node_kind", nodef(pdf.node_id)) \
		 .withColumn("now", from_unixtime(pdf.now_sec, "YYYY-MM-dd")) \
		 .withColumn("acquisition_era", when(regexp_extract(pdf.dataset_name, acquisition_era_reg, 1) == "", lit("null")).otherwise(regexp_extract(pdf.dataset_name, acquisition_era_reg, 1))) \
		 .withColumn("data_tier", when(regexp_extract(pdf.dataset_name, data_tier_reg, 1) == "", lit("null")).otherwise(regexp_extract(pdf.dataset_name, data_tier_reg, 1))) \
```
- Improved script performance. ~30 snapshots are processed in approximately 5 minutes (using yarn-client)
- Started developing delta function.
