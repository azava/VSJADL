"""
#################################### HEADER ####################################
#
#	JOB NAME: VERY SIMPLE JSON DATA ANALYZER
#	AUTHOR: ALEX ZAVA
#	DATE: 2021-07-02
#	CURRENT VERSION: 1.0
#	JOB DESCRIPTION: Simple Job in PySpark. Analyzes JSON Data, remove duplicates 
# 				   and save in parquet format partitioned by columns.
#
# 	VERSION			AUTHOR						DATE
# 	1.0				Alex Zava					2021-07-02
#
################################################################################
"""
from pyspark import SparkContext , HiveContext
from pyspark.sql import functions as f
from pyspark.sql import Window

from datetime import datetime

import os
import argparse


############################
# FUNCTIONS	
def read_data(path):		
	
	try:
		df = hc.read.json('{}/*.json'.format(path))
		
		print("Files Read".format(path))
		
		return df
	except Exception as e:
		print("Error reading path {}".format(path))
		print("Error: {}".format(e))
		
def remove_duplicates(df,order_column):
	
	normal_fields = df.columns
	normal_fields.remove(order_column)
	window = Window.partitionBy(normal_fields).orderBy(f.col(order_column).desc())
	
	df = df.withColumn('row_number', f.row_number().over(window))
	df = df.where(df["row_number"] == 1).drop("row_number")


	return df
		
def create_folder(folder):
	
	if not os.path.exists(folder):
		os.makedirs(folder)
		
def create_domain_folder(domain_list,base_path):
	
	for domain in domain_list:
		create_folder("{}/{}".format(base_path,domain))
		
def add_processed_time(df):

	df = df.withColumn("processed_timestamp", f.current_timestamp())
	
	return df

def save_data(df,base_path,partition_columns,order_column):
	
	df = df.withColumn("date" , f.date_format(df[order_column], "yyyy-MM-dd")) 
	
	df.write.format("parquet") \
					.mode("append") \
					.partitionBy(partition_columns.split(',')) \
					.save("{}".format(base_path))


def pipeline(input_path,output_path,order_column,partition_columns):
	
	# Reading path
	print("Reading path {}...".format(input_path))
	df = read_data(input_path)
	
	# Remove duplicates by timestamp
	print("Processing data...".format(input_path))
	df = remove_duplicates(df,order_column)
	
	# Add time of processment
	df = add_processed_time(df)
	
	# Save Data
	print("Saving data...".format(output_path))
	save_data(df,output_path,partition_columns,order_column)
	
	df.show()
	return 0
	
if __name__ == "__main__":

	# Read Arguments
	parser = argparse.ArgumentParser()
	parser.add_argument('--input_path',help="Path of input JSON Data",required=True)
	parser.add_argument('--output_path',help="Path of output Parquet Data",required=True)
	parser.add_argument('--order_column',help="Timestamp Column used as reference to remove duplicates.",required=True)
	parser.add_argument('--partition_columns',help="Unique columns  used in parquet partition. Sparate by comma. Ex.: type,day,time",required=True)
	args = parser.parse_args()

	# Start Context
	sc = SparkContext()
	hc = HiveContext(sc)
	
	# Run pipeline
	if pipeline(args.input_path,args.output_path,args.order_column,args.partition_columns) != 0:
		print("Execution Error!")

	print("Sucessful Execution!")

