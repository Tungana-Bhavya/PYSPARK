#### Setting Environment for SparkSession
#### How to Install PySpark Session ?
pip install pyspark

#### How to start a spark Session ?
from pyspark.sql import SparkSession; <br>
spark = SparkSession.builder.appName('Sample').getOrCreate()

#### Data Extraction
