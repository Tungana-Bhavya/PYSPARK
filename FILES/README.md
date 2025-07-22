## Setting Environment for SparkSession
#### - How to Install PySpark Session ?
pip install pyspark

#### - How to start a spark Session ?
from pyspark.sql import SparkSession; <br>
spark = SparkSession.builder.appName('Sample').getOrCreate()

## DataFrames
#### - How to create dataframe ?
df = spark.createDataFrame(data,schema)

#### - How to show dataframe ?
df.show()

#### - How to show dataframe ?
df.display()

#### - How to print schema ?
df.printSchema()

## Data Extraction
#### - How to read csv file ?
df = spark.read.csv('filepath', inferSchema = True, header = True)

#### - How to read json file ?
df = spark.read.json('filepath', inferSchema = True, header = True)

#### - How to read Parquet file ?
df = spark.read.parquet('filepath', inferSchema = True, header = True)

#### - How to read Database file ?
df = spark.read \
          .format("jdbc")\ </br>
          .option("url", "jdbc_url")\ </br>
          .option("dbtable", "table_name")\ </br>
          .option("user", "username")\ </br>
          .option("password","password")\ </br>
          .load() </br>

#### - How to read csv file using format ?
df = spark.read.format("csv").option('inferSchema', True).option('header', True).load("path_to_file.csv") </br>

## Data Transformation
### Data Types Conversion
#### - How to change type of data ?

<b>method 1:Using Cast() function</b></br>
from pyspark.sql.types import StringType, IntegerType, LongType, FloatType, DoubleType, BooleanType, TimestampType, DateType</br>

To string:
df = df.withColumn("column_name", col("column_name").cast("string"))</br>

To integer:
df = df.withColumn("column_name", col("column_name").cast("integer"))</br>

To long:
df = df.withColumn("column_name", col("column_name").cast("long"))</br>

To float:
df = df.withColumn("column_name", col("column_name").cast("float"))</br>

To double:
df = df.withColumn("column_name", col("column_name").cast("double"))</br>

To boolean:
df = df.withColumn("column_name", col("column_name").cast("boolean"))</br>

To timestamp:
df = df.withColumn("column_name", col("column_name").cast("timestamp"))</br>

To date:
df = df.withColumn("column_name", col("column_name").cast("date"))</br>

<b>method 2: Using StructType()</b></br>

from pyspark.sql.types import *</br>
from pyspark.sql.functions import * </br>
schema = StructType([</br>
StructField('column1', datatype(),True),
StructField('column2', datatype(), True)</br>
])

### Selection
#### - How to select columns from dataset ?
<b>method 1:</b></br>
df.select('col1', 'col2') </br>

<b>method 2:</b></br>
df.select(col('column1'), col('column2'))</br>

#### - How to select columns dynamically ?</br>
- list of columns to select</br>
columns = ["col1", "col2", "col3"]</br>
- Dynamically selecting columns</br>
df = df.select(*columns)

#### - How to select all columns and drop unrequired ?</br>
- selecting all columns except col_x and col_y </br>
df = df.select("*").drop("col_x", "col_y")


### Filtering
#### - How to filter data from dataset ?
<b>case 1:</b></br>
df.filter(df['column'] == value)
or
df.filter(col('column') == value)

<b>case 2:</b></br>
df.filter(col('column')==value) & (col('column2')< value)

<b>case 3:</b></br>
df.filter((col('column').isNull()) & (col('column').isin('val1', 'val2')))

### Joining
#### - How to apply joins on dataframes ?
inner join:</br>
joined_df = df1.join(df2, "key", "inner")</br>
or</br>
joined_df = df1.join(df2, col("df_key") == col("df2_key"), "inner") </br>

outer join:</br>
joined_df = df1.join(df2, "key", outer)</br>
or</br>
joined_df = df1.join(df2, col("df_key")==col("df2_key"), "outer")</br>

left join:</br>
joined_df = df1.join(df2, "key", "left")</br>
or </br>
joined_df = df1.join(df2, col("df_key")==col("df2_key"), "left")

right join:</br>
joined_df = df1.join(df2, col("df_key")==col("df2_key"), "right")</br>
joined_df = df1.join(df2, "key", "right")</br>

### Sorting
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
df = df.withColumn("new_column_name", concat(col("column1"), lit(""), col("column2")))</br>

df = df.withColumn("new_column_name", df['column'] + 15)   # calculating new_column_name from existing column by adding 15 

#### - How to apply limit function to the data ?
df.limit(value)  # df.limit(10)

## Data Manipulation
### Renaming
#### - How to rename old column ?
df.withColumnRenamed('old_name', 'new_name')

#### - How to rename multiple old columns ?
col_map = {</br>
'old_name_1' : 'new_name_1',</br>
'old_name_2' : 'new_name_2',</br>
'old_name_3' : 'new_name_3'</br>
}</br>

for old_name, new_name in col_map.items():</br>
df = df.withColumnRenamed(old_name, new_name)

#### - How to give alias name to the column ?
df.select(col('column').alias('column_alias'))

#### - How to remove duplicates from data ?
df.dropDuplicates()

### Handling Missing Values
### Droping
#### - How to drop column ?
df.drop['column_name'].display()

#### - How to drop multiple ?
df.drop('column_name_1', 'column_name_2').display()

#### - How to drop rows were all values are null ?
df.na.drop(how = 'all').show()</br>
or</br>
df.dropna(how ='all').show()
#### - How to drop rows were any value is null ?
df.na.drop(how = 'any').show()</br>
or</br>
df.dropna(how = 'any').show()

### Filling
#### - How to fill missing value ?
df.na.fill("value").show()</br>
or</br>
df.fillna("value").show()

#### - How to fill with specific value in specific column ?
df.na.fill({"col_name":"value", "col_name":"value"}).show()</br>
or</br>
df.fillna({"col_name":"value", "col_name":"value"}).show()

### Replacing
#### - How to replace existing value ?
df.na.replace(['old_value'],['new_value'])</br>
or</br>
df.replace('old_value', 'new_value', subset =['column_name'])

#### - How to replace existing value in multiple columns ?
df.replace({'column_1':{'old_value':'new_value'},</br>{'column_2' : {'old_value':'new_value'}})

### Conditional Statements
#### - How to apply conditional logic on data ?
df.withColumn('new_column_name', when((col('column_name_1') > condition </br>|(col('column_name_2'),"value_if_true").otherwise("value_if_not_true"))

#### - How to apply multiple conditional logic ?
df.withColumn('new_column_name', </br>
when(col('column_name') < condition, 'value_if_true')</br>
.when(col('column_name') == condition, 'value_if_true')</br>
.otherwise('value_if_not_true'))

#### - How to get unique records from dataframe ?
df.distinct.display()

#### - When to use countDistinct() function ?
scenario 1: with Select statement </br>
df = df.select(countDistinct("column_name").alias("new_distinct_column_name"))</br>

scenario 2: with aggregate function</br>
df = df.agg(countDistinct("column_name").alias("distinct_count"))</br>

scenario 3: with groupby and aggregate </br>
df = df.groupBy("group_column_name").agg(countDistinct("column_name").alias("distinct_column"))

#### - How to apply union function to the dataframe ?
- Requires the dataframes to have the same schema(i.e same names and data types) and the same column order. By using union missing column cannot be filled with null values.</br>

df1.union(df2)

#### - How to apply unionByName function to the dataframe ?
- Requires the dataframes to have the same column names and data types, but the column order cna be different. It combines dataframes based on the column names.</br>

df1.unionByName(df2, allowMissingColumns=True)

## Functions
### Date Functions
#### - How to get the current date column ?
df.withColumn('cur_date', current_date()).display()

#### - How to add specific number of days to the date column ?
df.withColumn('new_future_date', date_add('date_column', num_days)).display()

#### - How to subtract specific number of days to the date column ?
df.withColumn('new_past_date', date_sub('date_column', num_days)).display()

#### - How to apply datediff() function ?
df.withColumn('date_diff_column', datediff('end_date','start_date')).display()

#### - How to format date column ?
df.withColumn('my_formatted_date', date_format('date_column', 'format_string')).display()</br>
<b>--- format strings:</b></br>
yyyy : 4 digit year</br>
MM : 2 digit month</br>
dd : 2 digit day</br>
MMM : 3-letter month</br>
EEEE : weekday full_name

#### - How to split a specific column ?
df.withColumn('new_column_name', split('column_name', delimeter)).display()

#### - How to apply indexing to a specific column ?
--- indexing can be applied after conversion of data to list</br>
df.withColumn('new_column_name', split('column_name', delimeter)[index]).display()

### String Functions
#### - How to convert string to uppercase ?
df.select(upper('column_name').alias('uppercase_column'))

#### - How to convert string to lowercase ?
df.select(lower('column_name').alias('lowercase_column'))

#### - How to remove leading trailing spaces from string ?
df.select(trim('column_name').alias('trimmed_column'))

#### - How to remove leading whitespaces from a string ?
df.select(ltrim('column_name').alias('ltrimmed_column'))

#### - How to remove trailing whitespaces from a string ?
df.select(rtrim('column_name').alias(rtrimmed_column'))

#### - How to return length of string ?
df.select(length('column_name').alias('length_column'))

#### - How to concat two strings ?
df.select(concat('column_1','column_2').alias('concatenated_column'))

#### - How to concat two strings including space ?
df.select(concat_ws('column_1','column_2').alias('concatenated_column_with_space'))

#### - How to capitalize the first letter of string ?
df.select(initcap('column_name').alias('initcap_column'))

#### - How to split a string into an array of substrings ?
df.select(split('column_name',',').alias('split_column'))

