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

    def good_line(line):
        try:
            fields = line.split(',')
            if len(fields) != 15:
                return False
            int(fields[3])
            float(fields[7])
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
    
    transaction_clean_lines = transaction_lines.filter(good_line)
    
    transaction_rdd = transaction_clean_lines.map(lambda x: (x.split(',')[5], x.split(',')[6],  x.split(',')[7],  x.split(',')[11]))
    
    # Filter transactions where from_address is equal to to_address
    filtered_trans = transaction_rdd.filter(lambda x: x[0] == x[1])
    
    # Create an RDD where key is a tuple of (from_address, to_address) and value is the transaction value
    trans_rdd = filtered_trans.map(lambda x: ((x[0],x[1]), float(x[2])))
    
    # Reduce by key to get total transaction value for each (from_address, to_address) pair
    output = trans_rdd.reduceByKey(lambda x, y: x+y)
    
    # Take top 10 (from_address, to_address) pairs by transaction value in descending order
    top10 = output.takeOrdered(10, key = lambda x: -x[1])
    
    # Save the results to S3
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_PartD_washtrade' + date_time + '/topwashtrade.txt')
    my_result_object.put(Body=json.dumps(top10))
    


    spark.stop()
