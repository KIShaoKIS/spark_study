from pyspark import SparkConf
from pyspark.sql import SparkSession

def get_spark_session():
    spark_conf = SparkConf()
    ss = SparkSession \
            .builder \
            .appName("kishao") \
            .config(conf=spark_conf) \
            .getOrCreate()
    return ss
