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
            if len(fields)!=15:
                return False
            int(fields[11])
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
    
    def mappingfunc(line):
        value = line.split(',')[7]
        timestamp = line.split(',')[11]
        date = time.strftime("%m/%Y",time.gmtime(int(timestamp)))
        vals = float(value)
        return (date, (vals, 1))

    lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_lines=lines.filter(good_line)
    # num_transactions = clean_lines.map(lambda x: (time.strftime("%m/%Y",time.gmtime(int(x.split(',')[11]))), (int(x.split(',')[7], 1))
    # total_transactions = num_transactions.reduceByKey(operator.add)
    
    transactions_rdd = clean_lines.map(mappingfunc) 
    transactions_reducing = transactions_rdd.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])) 
    avg_transactions = transactions_reducing.map(lambda a: (a[0], str(a[1][0]/a[1][1]))).map(lambda l: ','.join(str(t) for t in l)) 
    

    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_' + date_time + '/avg_transactions.txt')
    my_result_object.put(Body=json.dumps(avg_transactions))


    spark.stop()
