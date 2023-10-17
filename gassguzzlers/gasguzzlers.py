import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Ethereum")\
        .getOrCreate()

    def good_line_contracts(line):
        try:
            fields = line.split(',')
            if len(fields)!=6:
                return False
            return True
        except:
            return False
        
        
    def good_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            int(fields[3])
            float(fields[9])
            float(fields[11])
            return True
        except:
            return False
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    

    transaction_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    contracts_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    
    transaction_clean_lines = transaction_lines.filter(good_line)
    contracts_clean_lines = contracts_lines.filter(good_line_contracts)
    # num_transactions = clean_lines.map(lambda b: (time.strftime("%m/%Y",time.gmtime(int(b.split(',')[11]))), 1))
    # total_transactions = num_transactions.reduceByKey(operator.add)
    
    transactions_rdd = transaction_clean_lines.map(lambda line: (time.strftime("%m/%Y",time.gmtime(int(line.split(',')[11]))), (float(line.split(',')[9]), 1)))
    transactions_reducing = transactions_rdd.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    avg_price = transactions_reducing.sortByKey(ascending=True).map(lambda a: (a[0], str(a[1][0]/a[1][1]))) 

    transactions_rdd1 = transaction_clean_lines.map(lambda line: (str(line.split(',')[6]), (time.strftime("%m/%Y",time.gmtime(int(line.split(',')[11]))), float(line.split(',')[8]))))
    contracts_rdd = contracts_clean_lines.map(lambda x: (x.split(',')[0],1))
    joins = transactions_rdd1.join(contracts_rdd)
    transcations_mapping1 = joins.map(lambda x: (x[1][0][0], (x[1][0][1],x[1][1])))
    transactions_reducing1 = transcations_mapping1.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    avg_gas_usage = transactions_reducing1.map(lambda a: (a[0], str(a[1][0]/a[1][1]))).sortByKey(ascending = True)
    
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_PartD_gasguzzlers' + date_time + '/avg_price.txt')
    my_result_object.put(Body=json.dumps(avg_price.collect()))
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_PartD_gasguzzlers' + date_time + '/avg_gas_used.txt')
    my_result_object.put(Body=json.dumps(avg_gas_usage.collect()))


    spark.stop()
