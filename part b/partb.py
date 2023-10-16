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
    
    transaction_rdd = transaction_clean_lines.map(lambda x: (x.split(',')[6], int(x.split(',')[7])))
    contracts_rdd = contracts_clean_lines.map(lambda x: (x.split(',')[0],1))
    transaction_reducing = transaction_rdd.reduceByKey(lambda x, y: x + y)
    transaction_contracts_join = transaction_reducing.join(contracts_rdd)
    address_value=transaction_contracts_join.map(lambda x: (x[0], x[1][0]))
    top10 = address_value.takeOrdered(10, key=lambda l: -1*l[1])
    

    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_PartB' + date_time + '/10_popular_services.txt')
    my_result_object.put(Body=json.dumps(top10))


    spark.stop()
