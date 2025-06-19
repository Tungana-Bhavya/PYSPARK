### Setting Environment for SparkSession
#### - How to Install PySpark Session ?
pip install pyspark

#### - How to start a spark Session ?
from pyspark.sql import SparkSession; <br>
spark = SparkSession.builder.appName('Sample').getOrCreate()

### DataFrames
#### - How to create dataframe ?
df = spark.createDataFrame(data,schema)

#### - How to show dataframe ?
df.show()

#### - How to show dataframe ?
df.display()

#### - How to print schema ?
df.printSchema()

### Data Extraction
#### - How to read csv file ?
df = spark.read.csv('filepath', inferSchema = True, header = True)

#### - How to read json file ?
df = spark.read.json('filepath', inferSchema = True, header = True)

#### - How to read Parquet file ?
df = spark.read.parquet('filepath', inferSchema = True, header = True)

#### - How to read Database file ? </br>
df = spark.read \
          .format("jdbc")\ </br>
          .option("url", "jdbc_url")\ </br>
          .option("dbtable", "table_name")\ </br>
          .option("user", "username")\ </br>
          .option("password","password")\ </br>
          .load() </br>

### Data Transformation
#### - How to select columns from dataset ?
<b>method 1:</b></br>
df.select('col1', 'col2') </br>

<b>method 2:</b></br>
df.select(col('column1'), col('column2'))</br>

#### - How to filter data from dataset ?
<b>case 1:</b></br>
df.filter(df['column'] == value)
or
df.filter(col('column') == value)

<b>case 2:</b></br>
df.filter(col('column')==value) & (col('column2')< value)

<b>case 3:</b></br>
df.filter((col('column').isNull()) & (col('column').isin('val1', 'val2')))

#### - How to sort data in ascending order ?
<b>By using sort():</b></br>
df = df.sort("column_name")</br>

<b>By using orderBy():</b></br>
df = df.orderBy("column_name")

#### - How to sort data in descending order ?
<b>By using sort():</b></br>
df = df.sort(df['column_name'].desc())</br>

<b>By using orderBy():</b></br>
df = df.orderBy(df['column_name'].desc())

#### - How to sort multiple columns in ascending order ?
df = df.orderBy(['column1','column2'])</br>
df = df.orderBy(df['column1'].desc(), df['column2']) # column1 in descending order and column2 in ascending order

#### - How to group and aggregate the given data ?
df.groupBy("column_name").agg({'column_2':'sum'})

#### - How to add new columns to the data ?
 - Concat function for string concatenation and col for referncing existing columns.</br>
df = df.withColumn("new_column_name", concat(col("column1"), lit(""), col("column2")))
df = df.withColumn("new_column_name", df['column'] + 15)   # calculating new_column_name from existing column by adding 15 

#### - How to apply limit function to the data ?
df.limit(value)  # df.limit(10)

### Data Manipulation
#### - How to rename new columns ?
df.withColumnRenamed('old_name', 'new_name')

#### - How to give alias name to the column ?
df.select(col('column').alias('column_alias'))

#### - How to remove duplicates from data ?
df.dropDuplicates()






