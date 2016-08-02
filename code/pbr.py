#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       		: pbr.py
Author     		: Aurimas Repecka <aurimas.repecka AT gmail dot com>
Based On Work By   	: Valentin Kuznetsov <vkuznet AT gmail dot com>
Description:
    http://stackoverflow.com/questions/29936156/get-csv-to-spark-dataframe
"""

# system modules
import os
import sys
import argparse

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField, StringType
from pyspark.sql.functions import udf, from_unixtime, date_format, regexp_extract, when, lit

import re
from datetime import datetime as dt

# additional data needed for joins
GROUP_CSV_PATH = "additional_data/phedex_groups.csv"												# user group names
NODE_CSV_PATH = "additional_data/phedex_node_kinds.csv"												# node kinds

AGGREGATIONS = ["sum", "count", "min", "max", "first", "last", "mean"]  							# supported aggregation functions
GROUPKEYS = ["now", "dataset_name", "block_name", "node_name", "br_is_custiodial", "br_user_group",
			"data_tier", "acquisition_era", "node_kind", "now_sec"]									# supported group key values
GROUPRES = ["block_files", "block_bytes", "br_src_files", "br_src_bytes", "br_dest_files", 
			"br_dest_bytes", "br_node_files", "br_node_bytes", "br_xfer_files", "br_xfer_bytes"] 	# supported group result values

class OptionParser():
	def __init__(self):
		"User based option parser"
		self.parser = argparse.ArgumentParser(prog='PROG')
		msg = "Input data file on HDFS, e.g. hdfs:///path/data/file"
		self.parser.add_argument("--fname", action="store",
			dest="fname", default="", help=msg)
		msg = 'Output file on HDFS, e.g. hdfs:///path/data/output.file'
		self.parser.add_argument("--fout", action="store",
			dest="fout", default="", help=msg)
		self.parser.add_argument("--verbose", action="store_true",
			dest="verbose", default=False, help="Be verbose")
		self.parser.add_argument("--yarn", action="store_true",
			dest="yarn", default=False, help="Be yarn")
		self.parser.add_argument("--basedir", action="store",
			dest="basedir", default="/project/awg/cms/phedex/block-replicas-snapshots/csv/", help="Base directory of snapshots")
		self.parser.add_argument("--fromdate", action="store",
			dest="fromdate", default="", help="Filter by start date")
		self.parser.add_argument("--todate", action="store",
			dest="todate", default="", help="Filter by end date")
		self.parser.add_argument("--keys", action="store",
			dest="keys", default="dataset_name, node_name", help="Names (csv) of group keys to use, supported keys: %s" % GROUPKEYS)
		self.parser.add_argument("--results", action="store",
			dest="results", default="block_files, block_bytes", help="Names (csv) of group results to use, supported results: %s" % GROUPRES)
		self.parser.add_argument("--aggregations", action="store",
			dest="aggregations", default="sum", help="Names (csv) of aggregation functions to use, supported aggregations: %s" % AGGREGATIONS)
		self.parser.add_argument("--order", action="store",
			dest="order", default="", help="Column names (csv) for ordering data")
		self.parser.add_argument("--asc", action="store",
			dest="asc", default="", help="1 or 0 (csv) for ordering columns (0-desc, 1-asc)")
		self.parser.add_argument("--header", action="store_true",
			dest="header", default=False, help="Print header in the first file of csv")

def schema():
	return StructType([StructField("now_sec", DoubleType(), True),
		 			 StructField("dataset_name", StringType(), True),
 					 StructField("dataset_id", IntegerType(), True),
	 			 	 StructField("dataset_is_open", StringType(), True),
		 			 StructField("dataset_time_create", DoubleType(), True),
		 			 StructField("dataset_time_update", DoubleType(), True),
		 			 StructField("block_name", StringType(), True), 
		 			 StructField("block_id", IntegerType(), True),
		 			 StructField("block_files", IntegerType(), True),
		 			 StructField("block_bytes", DoubleType(), True),
		 			 StructField("block_is_open", StringType(), True),
		 			 StructField("block_time_create", DoubleType(), True),
		 			 StructField("block_time_update", DoubleType(), True),
		 			 StructField("node_name", StringType(), True),
					 StructField("node_id", IntegerType(), True),
		 			 StructField("br_is_active", StringType(), True),
		 			 StructField("br_src_files", IntegerType(), True),
		 			 StructField("br_src_bytes", DoubleType(), True),
		 			 StructField("br_dest_files", IntegerType(), True),
		 			 StructField("br_dest_bytes", DoubleType(), True),
		 			 StructField("br_node_files", IntegerType(), True),
		 			 StructField("br_node_bytes", DoubleType(), True),
		 			 StructField("br_xfer_files", IntegerType(), True),
		 			 StructField("br_xfer_bytes", DoubleType(), True),
		 			 StructField("br_is_custodial", StringType(), True),
		 			 StructField("br_user_group_id", IntegerType(), True),
		 			 StructField("replica_time_create", DoubleType(), True),
		 			 StructField("replica_time_updater", DoubleType(), True)])

# get dictionaries needed for joins
def getJoinDic():   
	groupdic = {None : "null"}
	with open(GROUP_CSV_PATH) as fg:
		for line in fg.read().splitlines():
			(gid, gname) = line.split(',')
			groupdic[int(gid)] = gname

	nodedic = {None : "null"}
	with open(NODE_CSV_PATH) as fn:
		for line in fn.read().splitlines():
			data = line.split(',')
			nodedic[int(data[0])] = data[2] 

	return groupdic, nodedic  

# get file list by dates
def getFileList(basedir, fromdate, todate):
	dirs = os.popen("hadoop fs -ls %s | sed '1d;s/  */ /g' | cut -d\  -f8" % basedir).read().splitlines()
	# if files are not in hdfs --> dirs = os.listdir(basedir)
	
	try:
		fromdate = dt.strptime(fromdate, "%Y-%m-%d")
		todate = dt.strptime(todate, "%Y-%m-%d")
	except ValueError as err:
		raise ValueError("Unparsable date parameters. Date should be specified in form: YYYY-mm-dd")		
 		
	pattern = re.compile(r"(\d{4}-\d{2}-\d{2})")
   
	dirdate_dic = {}
	for di in dirs:
		matching = pattern.search(di)
		if matching:
			dirdate_dic[di] = dt.strptime(matching.group(1), "%Y-%m-%d")

	# if files are not in hdfs --> return [ basedir + k for k, v in dirdate_dic.items() if v >= fromdate and v <= todate]
	return [k for k, v in dirdate_dic.items() if v >= fromdate and v <= todate]	

# validate aggregation parameters
def validateAggregationParams(keys, res, agg, order):
	unsup_keys = set(keys).difference(set(GROUPKEYS)) 
	unsup_res = set(res).difference(set(GROUPRES))
	unsup_agg = set(agg).difference(set(AGGREGATIONS))
	unsup_ord = set(order).difference(set(keys + res)) if order != [''] else None
	
	msg = ""
	if unsup_keys:
		msg += 'Group key(s) = "%s" are not supported. ' % toStringVal(unsup_keys)
	if unsup_res:
		msg += 'Group result(s) = "%s" are not supported. ' % toStringVal(unsup_res)
	if unsup_agg:
		msg += 'Aggregation function(s) = "%s" are not supported. ' % toStringVal(unsup_agg)
	if unsup_ord:
		msg += 'Order key(s) = "%s" are not available. ' % toStringVal(unsup_ord)
	if msg:
		raise NotImplementedError(msg)

# validate dates and fill default values		
def defDates(fromdate, todate):
	if not fromdate or not todate:
		fromdate = dt.strftime(dt.now(), "%Y-%m-%d")
		todate = dt.strftime(dt.now(), "%Y-%m-%d")
	return fromdate, todate

# creating results and aggregation dictionary
def zipResultAgg(res, agg):
	return dict(zip(res, agg)) if len(res) == len(agg) else dict(zip(res, agg * len(res)))

# form ascennding and order arrays according aggregation functions
def formOrdAsc(order, asc, resAgg_dic):
	asc = map(int, asc) if len(order) == len(asc) else [1] * len(order)
	orderN = [resAgg_dic[orde] + "(" + orde + ")" if orde in resAgg_dic.keys() else orde for orde in order] 
	return orderN, asc

# unions all files in one dataframe
def unionAll(dfs):
	return reduce(DataFrame.unionAll, dfs)


#########################################################################################################################################

def main():
	"Main function"
	optmgr  = OptionParser()
	opts = optmgr.parser.parse_args()

	# setup spark/sql context to be used for communication with HDFS
	sc = SparkContext(appName="phedex_br")
	if not opts.yarn:
		sc.setLogLevel("ERROR")
	sqlContext = SQLContext(sc)

	schema_def = schema()

	# read given file(s) into RDD
	if opts.fname:
		pdf = sqlContext.read.format('com.databricks.spark.csv')\
						.options(treatEmptyValuesAsNulls='true', nullValue='null')\
						.load(opts.fname, schema = schema_def)
	elif opts.basedir:
		fromdate, todate = defDates(opts.fromdate, opts.todate)
		files = getFileList(opts.basedir, fromdate, todate)
		msg = "Between dates %s and %s found %d directories" % (fromdate, todate, len(files))
		print msg

		if not files:
			return
		pdf = unionAll([sqlContext.read.format('com.databricks.spark.csv')
						.options(treatEmptyValuesAsNulls='true', nullValue='null')\
						.load(file_path, schema = schema_def) \
						for file_path in files])
	else:
		raise ValueError("File or directory not specified. Specify fname or basedir parameters.")
	
	# parsing additional data (to given data adding: group name, node kind, acquisition era, data tier, now date)
	groupdic, nodedic = getJoinDic()
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
			 .withColumn("data_tier", when(regexp_extract(pdf.dataset_name, data_tier_reg, 1) == "", lit("null")).otherwise(regexp_extract(pdf.dataset_name, data_tier_reg, 1)))

	# print dataframe schema
	if opts.verbose:
		ndf.show()
		print("pdf data type", type(ndf))
		ndf.printSchema()

	# process aggregation parameters
	keys = [key.lower().strip() for key in opts.keys.split(',')]
	results = [result.lower().strip() for result in opts.results.split(',')]
	aggregations = [agg.strip() for agg in opts.aggregations.split(',')]
	order = [orde.strip() for orde in opts.order.split(',')] if opts.order else []
	asc = [asce.strip() for asce in opts.asc.split(',')] if opts.order else []

	validateAggregationParams(keys, results, aggregations, order)
	
	resAgg_dic = zipResultAgg(results, aggregations)
	order, asc = formOrdAsc(order, asc, resAgg_dic)

	# perform aggregation
	if order:
		aggres = ndf.groupBy(keys).agg(resAgg_dic).orderBy(order, ascending=asc)
	else:
		aggres = ndf.groupBy(keys).agg(resAgg_dic)

	# output results
	if opts.fout:
		if opts.header:
			aggres.write.format('com.databricks.spark.csv').options(header = 'true').save(opts.fout)
		else:
			aggres.write.format('com.databricks.spark.csv').save(opts.fout)
	else:
		aggres.show(15)

if __name__ == '__main__':
	main()

