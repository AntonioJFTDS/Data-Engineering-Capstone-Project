import pandas as pd

from pyspark.sql.types import (
    StringType, BooleanType, IntegerType, FloatType, DateType
)

from pyspark.sql.window import Window

import pyspark.sql.functions as F
from pyspark.sql.functions import row_number, lower, col, lit
from pyspark.sql.functions import sum, max, min, avg, count, mean

from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date
import pyspark.sql.types as TS


from pyspark.sql import SparkSession
spark = SparkSession.builder.\
config("spark.jars.repositories", "https://repos.spark-packages.org/").\
config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
appName('Pivot() Stack() PySpark'). \
enableHiveSupport().getOrCreate()

###################################################################################################

# political_stability_country.csv

# 1. create "political_stability_country"

def create_dataframe_political_stability_country(spark,filePath):

    political_stability_country = spark.read.format("csv").option("header","true").load(filePath)

    political_stability_country = political_stability_country.drop('series Name','Series Code')

    # change type of all the years column
    for col_name in political_stability_country.columns[2:]:
        political_stability_country = political_stability_country \
        .withColumn(col_name,political_stability_country[col_name].cast(FloatType())) 
        
    return political_stability_country

# 2. create "political_stability_country_unpivot" from "political_stability_country"

def create_dataframe_political_stability_country_unpivot(political_stability_country):
    
    # unpivot "political_stability_country"
    political_stability_country_unpivot = political_stability_country \
        .select(political_stability_country.columns[:3]) \
        .toDF("country_name","country_code","pss").withColumn("year", lit(political_stability_country.columns[2]))

    # pill up the succesives new_dfs (unpivoted dataframes) to current political_stability_country_unpivot
    for col_name in political_stability_country.columns[3:]:
        new_df = political_stability_country.select(['Country Name', 'Country Code',col_name]).toDF("country_name","country_code","pss").withColumn("year", lit(col_name))
        political_stability_country_unpivot = political_stability_country_unpivot.union(new_df)

    # clean up 
    political_stability_country_unpivot = political_stability_country_unpivot \
        .withColumn("year",political_stability_country_unpivot["year"].cast(IntegerType())) \
        .dropDuplicates()
    
    return political_stability_country_unpivot

###################################################################################################

# unemployment_country.csv

# 1. create "unemployment_country"

def create_dataframe_unemployment_country(spark,filePath):

    filePath = 'unemployment_country.csv'
    unemployment_country=spark.read.format("csv").option("header","true").load(filePath)

    # drop('Series Code') 
    unemployment_country = unemployment_country.drop('Series Code') 

    # change columns ["1960","1961","1962",...] type to Float
    for col_name in unemployment_country.columns[3:]:
        unemployment_country = unemployment_country \
        .withColumn(col_name,unemployment_country[col_name].cast(FloatType())) 
        
    return unemployment_country

# 2.create "unemployment_country_short_unpivot" from "unemployment_country"

def create_dataframe_unemployment_country_short_unpivot(unemployment_country):

    # only keep columns [Country Name|Country Code| Series Name|1960] ie the 4 first columns , from unemployment_country
    # rename columns
    # add a fifth colmn called "year" with all rows same value = name of 4th column
    unemployment_country_short = unemployment_country \
        .select(unemployment_country.columns[:4]) \
        .toDF("country_name","country_code","Series Name","perct") \
        .withColumn("year", lit(unemployment_country.columns[3]))

    # spread horizontally the combination ("Series Name","perct")
    unemployment_country_short_unpivot = unemployment_country_short. \
        groupBy("country_name","country_code","year"). \
        pivot("Series Name"). \
        sum("perct"). \
        drop("null")

    # for each column starting from the 4th column create a new data frame "new_df_short_unpivot" 
    # and pill it up to the current "unemployment_country_short_unpivot"
    for col_name in unemployment_country.columns[4:]:
        # only keep columns [Country Name|Country Code| Series Name|1960] ie the 4 first columns , from unemployment_country
        # rename columns
        # add a fifth colmn called "year" with all rows same value = name of 4th column
        new_df_short = unemployment_country \
            .select(["Country Name","Country Code","Series Name",col_name]) \
            .toDF("country_name","country_code","Series Name","perct") \
            .withColumn("year", lit(col_name))

        # spread horizontally the combination ("Series Name","perct")
        new_df_short_unpivot = new_df_short. \
            groupBy("country_name","country_code","year"). \
            pivot("Series Name"). \
            sum("perct"). \
            drop("null")

        # unioned unemployment_country_short_unpivot with new_df_short_unpivot
        unemployment_country_short_unpivot = unemployment_country_short_unpivot.union(new_df_short_unpivot)

    # clean up:
    unemployment_country_short_unpivot = unemployment_country_short_unpivot \
        .withColumn("year",unemployment_country_short_unpivot["year"].cast(IntegerType())) \
        .toDF("country_name","country_code","year","uwae","uwbe","uwie","ut") \
        .dropDuplicates()
    
    return unemployment_country_short_unpivot

###################################################################################################

# population_country.csv

# create population_country 

def create_dataframe_population_country(spark,filePath):

    population_country=spark.read.format("csv").option("header","true").load(filePath)
    
    # population_country columns Country Name|Year|   Count
    
    # rename columns
    population_country = population_country.withColumnRenamed("Country Name","country_name") \
        .withColumnRenamed("Year","year") \
        .withColumnRenamed("Count","count")
    
    # cast type 
    population_country = population_country.withColumn("country_name",population_country["country_name"].cast(StringType())) \
         .withColumn("year",population_country["year"].cast(IntegerType())) \
         .withColumn("count",population_country["count"].cast(IntegerType())) 

    population_country = population_country.withColumn('powp',F.col('count')/F.sum('count').over(Window.partitionBy('year'))*100) 

    population_country = population_country.dropDuplicates()
    
    return population_country


###################################################################################################

#  create the final table economics_country

# 1. create union_name_year which a distinct list of country name and years of the 3 dataframes

def create_dataframe_union_name_year(political_stability_country_unpivot,unemployment_country_short_unpivot,population_country):
    
    # union_name_year is a distinct list of country name and year from 3 dataframes
    union_name_year = political_stability_country_unpivot.select('country_name','year') \
        .union(unemployment_country_short_unpivot.select('country_name','year')) \
        .union(population_country.select('country_name','year')) \
    
    # clean up
    union_name_year = union_name_year \
        .toDF("u_name","u_year") \
        .dropDuplicates()
    
    return union_name_year


# 2. create economics_country_no_labels by joining union_name_year with the 3 dataframes

def create_dataframe_economics_country_no_labels(union_name_year,political_stability_country_unpivot,unemployment_country_short_unpivot,population_country):
    
    economics_country_no_labels= union_name_year. \
        join(political_stability_country_unpivot, \
             [union_name_year.u_name == political_stability_country_unpivot.country_name, \
              union_name_year.u_year == political_stability_country_unpivot.year], \
             "outer") \
        .join(unemployment_country_short_unpivot, \
              [union_name_year.u_name == unemployment_country_short_unpivot.country_name, \
               union_name_year.u_year == unemployment_country_short_unpivot.year], \
              "outer") \
        .join(population_country, \
              [union_name_year.u_name == population_country.country_name, \
               union_name_year.u_year == population_country.year], \
              "outer") 

    # clean up
    economics_country_no_labels = economics_country_no_labels.select("u_name","u_year","pss","uwae","uwbe","uwie","ut","powp") \
        .toDF("country_name","year","pss","uwae","uwbe","uwie","ut","powp") \
        .withColumn('country_name', lower(col('country_name'))) \
        .dropDuplicates()
    
    return economics_country_no_labels

def create_dataframe_label_country(spark):

    with open('./I94_SAS_Labels_Descriptions.SAS') as f:
        f_content = f.read()
        f_content = f_content.replace('\t', '')
    def code_mapper(file, idx):
        f_content2 = f_content[f_content.index(idx):]
        f_content2 = f_content2[:f_content2.index(';')].split('\n')
        f_content2 = [i.replace("'", "") for i in f_content2]
        dic = [i.split('=') for i in f_content2[1:]]
        dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
        return dic
    i94cit_res = code_mapper(f_content, "i94cntyl")
    i94port = code_mapper(f_content, "i94prtl")
    i94mode = code_mapper(f_content, "i94model")
    i94addr = code_mapper(f_content, "i94addrl")
    i94visa = {'1':'Business','2': 'Pleasure','3' : 'Student'}

    #i94addr
    label_country = pd.Series(i94cit_res,name='country_name')
    label_country.index.name = 'country_code'
    label_country.reset_index()
    label_country = label_country.to_frame()
    label_country = label_country.reset_index()
    label_country['country_name'] = label_country['country_name'].str.lower()
    label_country = spark.createDataFrame(label_country)
    
    return label_country

def create_dataframe_economics_country(economics_country_no_labels,label_country):
    
    # outer join economics_country_no_labels to label_country by 
    # economics_country_no_labels has more rows than label_country
    economics_country = economics_country_no_labels. \
        join(label_country, \
             [economics_country_no_labels.country_name == label_country.country_name], \
             "left")

    # clean up
    economics_country = economics_country \
        .withColumn("country_code",economics_country["country_code"].cast(IntegerType())) \
        .toDF('country_name', 'year', 'pss', 'uwae', 'uwbe', 'uwie', 'ut', 'powp', 'country_code', 'country_name_2') \
        .drop('country_name_2') \
        .na.drop(subset=["country_code","year","country_code"])
        
    # create an unique id column
    economics_country = economics_country. \
        select('country_name', 'year', 'pss', 'uwae', 'uwbe', 'uwie', 'ut', 'powp', 'country_code', \
               F.row_number().over(Window.partitionBy().orderBy(["year","country_name"])).alias("id_economics_country"))
    
#     # test
#     print("economics_country")
#     print(economics_country)
#     print(type(economics_country))
#     economics_country.show()
#     print(economics_country.count())

    return economics_country

def final_create_dataframe_economics_country(spark,political_stability_country_path,unemployment_country_path,population_country_path):

    political_stability_country = create_dataframe_political_stability_country(spark,political_stability_country_path)
    political_stability_country_unpivot = create_dataframe_political_stability_country_unpivot(political_stability_country)

    unemployment_country = create_dataframe_unemployment_country(spark,unemployment_country_path)
    unemployment_country_short_unpivot = create_dataframe_unemployment_country_short_unpivot(unemployment_country)

    population_country = create_dataframe_population_country(spark,population_country_path)

    union_name_year = create_dataframe_union_name_year(political_stability_country_unpivot,unemployment_country_short_unpivot,population_country)

    economics_country_no_labels = create_dataframe_economics_country_no_labels(union_name_year,political_stability_country_unpivot,unemployment_country_short_unpivot,population_country)

    label_country = create_dataframe_label_country(spark)
    
    economics_country = create_dataframe_economics_country(economics_country_no_labels,label_country)
    
    return economics_country

###################################################################################################


def create_dataframe_label_us_state(spark):

    with open('./I94_SAS_Labels_Descriptions.SAS') as f:
        f_content = f.read()
        f_content = f_content.replace('\t', '')
    def code_mapper(file, idx):
        f_content2 = f_content[f_content.index(idx):]
        f_content2 = f_content2[:f_content2.index(';')].split('\n')
        f_content2 = [i.replace("'", "") for i in f_content2]
        dic = [i.split('=') for i in f_content2[1:]]
        dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
        return dic
    i94cit_res = code_mapper(f_content, "i94cntyl")
    i94port = code_mapper(f_content, "i94prtl")
    i94mode = code_mapper(f_content, "i94model")
    i94addr = code_mapper(f_content, "i94addrl")
    i94visa = {'1':'Business','2': 'Pleasure','3' : 'Student'}

    #i94addr
    label_us_state = pd.Series(i94addr,name='state_name')
    label_us_state.index.name = 'state_code'
    label_us_state.reset_index()
    label_us_state = label_us_state.to_frame()
    label_us_state = label_us_state.reset_index()
    label_us_state['state_name'] = label_us_state['state_name'].str.lower()
    label_us_state = spark.createDataFrame(label_us_state)

    return label_us_state

def create_dataframe_economics_us_state_no_labels(spark,filePath):
    economics_us_state_no_labels = spark.read.format("csv").option("header","true").load(filePath)
  
      # original columns : |Year|Month|State|County|Rate|

    # convert Month to numerical month
    economics_us_state_no_labels = economics_us_state_no_labels \
        .withColumn("Month",F.from_unixtime(F.unix_timestamp(col("Month"),'MMMM'),'MM'))

    # cast type columns
    economics_us_state_no_labels = economics_us_state_no_labels \
        .withColumn("Year",economics_us_state_no_labels["Year"].cast(IntegerType())) \
        .withColumn("Month",economics_us_state_no_labels["Month"].cast(IntegerType())) \
        .withColumn("Rate",economics_us_state_no_labels["Rate"].cast(FloatType())) 
    
    # average ut by County
    economics_us_state_no_labels = economics_us_state_no_labels.groupBy("Year","Month","State").avg("Rate")
    
    # clean up
    economics_us_state_no_labels = economics_us_state_no_labels \
        .withColumn('State', lower(col('State'))) \
        .toDF('year','month','state_name','ut') \
        .dropDuplicates()
    
    return economics_us_state_no_labels

def create_dataframe_economics_us_state(economics_us_state_no_labels,label_us_state):
    
    # outer join economics_us_state_no_labels to label_us_state by 
    # economics_us_state_no_labels has more rows than label_us_state
    economics_us_state = economics_us_state_no_labels. \
        join(label_us_state, \
             [economics_us_state_no_labels.state_name == label_us_state.state_name], \
             "left")
    
    # clean up
    economics_us_state = economics_us_state \
        .toDF('year','month','state_name','ut','state_code','state_name_2') \
        .drop('state_name_2') \
        .na.drop(how="any")
    
    # create an unique id column
    economics_us_state = economics_us_state. \
        select('year','month','state_name','ut','state_code', \
               F.row_number().over(Window.partitionBy().orderBy(['year','state_name','month'])).alias("id_economics_us_state"))
    
#     # test
#     print("economics_us_state")
#     print(economics_us_state)
#     print(type(economics_us_state))
#     economics_us_state.show()
#     print(economics_us_state.count())
    
    return economics_us_state

def final_create_dataframe_economics_us_state(path):
    economics_us_state_no_labels = create_dataframe_economics_us_state_no_labels(spark,path)
    label_us_state = create_dataframe_label_us_state(spark)
    economics_us_state = create_dataframe_economics_us_state(economics_us_state_no_labels,label_us_state)
    return economics_us_state


###################################################################################################

def create_dataframe_temperature_country_no_labels(spark,filePath):
    
    temperature_country_no_labels = spark.read.format("csv").option("header","true").load(filePath)
  
    # original columns : Region|Country|State|City|Month|Day|Year|AvgTemperature
    
    # only keep the rows where Country = US
    temperature_country_no_labels = temperature_country_no_labels\
        .filter(col('Country')!='US')
    
    # cast type columns
    temperature_country_no_labels = temperature_country_no_labels \
        .withColumn("Year",temperature_country_no_labels["Year"].cast(IntegerType())) \
        .withColumn("Month",temperature_country_no_labels["Month"].cast(IntegerType())) \
        .withColumn("AvgTemperature",temperature_country_no_labels["AvgTemperature"].cast(FloatType())) 

    # average daily Temperature by "Year","State","Month"
    temperature_country_no_labels = temperature_country_no_labels \
        .groupBy("Year","Country","Month") \
        .agg(avg("AvgTemperature"),min("AvgTemperature"),max("AvgTemperature"))
    
    # clean up
    temperature_country_no_labels = temperature_country_no_labels \
        .withColumn('Country', lower(col('Country'))) \
        .toDF('year','country_name','month','temperature','min_temperature','max_temperature') \
        .dropDuplicates()

#     # test
#     print("temperature_country_no_labels")
#     print(temperature_country_no_labels)
#     print(type(temperature_country_no_labels))
#     temperature_country_no_labels.show()
#     print(temperature_country_no_labels.count())
    
    return temperature_country_no_labels



def create_dataframe_label_country(spark):

    with open('./I94_SAS_Labels_Descriptions.SAS') as f:
        f_content = f.read()
        f_content = f_content.replace('\t', '')
    def code_mapper(file, idx):
        f_content2 = f_content[f_content.index(idx):]
        f_content2 = f_content2[:f_content2.index(';')].split('\n')
        f_content2 = [i.replace("'", "") for i in f_content2]
        dic = [i.split('=') for i in f_content2[1:]]
        dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
        return dic
    i94cit_res = code_mapper(f_content, "i94cntyl")
    i94port = code_mapper(f_content, "i94prtl")
    i94mode = code_mapper(f_content, "i94model")
    i94addr = code_mapper(f_content, "i94addrl")
    i94visa = {'1':'Business','2': 'Pleasure','3' : 'Student'}

    #i94addr
    label_country = pd.Series(i94cit_res,name='country_name')
    label_country.index.name = 'country_code'
    label_country.reset_index()
    label_country = label_country.to_frame()
    label_country = label_country.reset_index()
    label_country['country_name'] = label_country['country_name'].str.lower()
    label_country = spark.createDataFrame(label_country)
    
    return label_country



#create_dataframe_temperature_country_no_labels(spark,'temperature_country_country.csv')

def create_dataframe_temperature_country(temperature_country_no_labels,label_country):
    
    # outer join temperature_country_no_labels to label_country by 
    # temperature_country_no_labels has more rows than label_country
    temperature_country = temperature_country_no_labels. \
        join(label_country, \
             [temperature_country_no_labels.country_name == label_country.country_name], \
             "left")
    
    # clean up
    temperature_country = temperature_country \
        .withColumn("country_code",temperature_country["country_code"].cast(IntegerType())) \
        .toDF('year','country_name','month','temperature','min_temperature','max_temperature','country_code','country_name_2') \
        .drop('country_name_2') \
        .na.drop(how="any")
    
    # create an unique id column
    temperature_country = temperature_country. \
        select('year','country_name','month','temperature','min_temperature','max_temperature','country_code', \
               F.row_number().over(Window.partitionBy().orderBy(['year','country_name','month'])).alias("id_temperature_country"))
    
#     # test
#     print("temperature_country")
#     print(temperature_country)
#     print(type(temperature_country))
#     temperature_country.show()
#     print(temperature_country.count())
    
    return temperature_country

def final_create_dataframe_temperature_country(spark,filePath):
    temperature_country_no_labels = create_dataframe_temperature_country_no_labels(filePath)
    label_country = create_dataframe_label_country(spark)
    temperature_country = create_dataframe_temperature_country(temperature_country_no_labels,label_country)
    return temperature_country


###################################################################################################

def create_dataframe_label_us_state(spark):

    with open('./I94_SAS_Labels_Descriptions.SAS') as f:
        f_content = f.read()
        f_content = f_content.replace('\t', '')
    def code_mapper(file, idx):
        f_content2 = f_content[f_content.index(idx):]
        f_content2 = f_content2[:f_content2.index(';')].split('\n')
        f_content2 = [i.replace("'", "") for i in f_content2]
        dic = [i.split('=') for i in f_content2[1:]]
        dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
        return dic
    i94cit_res = code_mapper(f_content, "i94cntyl")
    i94port = code_mapper(f_content, "i94prtl")
    i94mode = code_mapper(f_content, "i94model")
    i94addr = code_mapper(f_content, "i94addrl")
    i94visa = {'1':'Business','2': 'Pleasure','3' : 'Student'}

    #i94addr
    label_us_state = pd.Series(i94addr,name='state_name')
    label_us_state.index.name = 'state_code'
    label_us_state.reset_index()
    label_us_state = label_us_state.to_frame()
    label_us_state = label_us_state.reset_index()
    label_us_state['state_name'] = label_us_state['state_name'].str.lower()
    label_us_state = spark.createDataFrame(label_us_state)

    return label_us_state

def create_dataframe_temperature_us_state_no_labels(spark,filePath):
    
    temperature_us_state_no_labels = spark.read.format("csv").option("header","true").load(filePath)
  
    # original columns : Region|Country|State|City|Month|Day|Year|AvgTemperature
    
    # only keep the rows where Country = US
    temperature_us_state_no_labels = temperature_us_state_no_labels\
        .filter(col('Country')=='US')
    
    # cast type columns
    temperature_us_state_no_labels = temperature_us_state_no_labels \
        .withColumn("Year",temperature_us_state_no_labels["Year"].cast(IntegerType())) \
        .withColumn("Month",temperature_us_state_no_labels["Month"].cast(IntegerType())) \
        .withColumn("AvgTemperature",temperature_us_state_no_labels["AvgTemperature"].cast(FloatType())) 

    # average daily Temperature by "Year","State","Month"
    temperature_us_state_no_labels = temperature_us_state_no_labels \
        .groupBy("Year","State","Month") \
        .agg(avg("AvgTemperature"),min("AvgTemperature"),max("AvgTemperature"))
    
    # clean up
    temperature_us_state_no_labels = temperature_us_state_no_labels \
        .withColumn('State', lower(col('State'))) \
        .toDF('year','state_name','month','temperature','min_temperature','max_temperature') \
        .dropDuplicates()
    
    return temperature_us_state_no_labels

#create_dataframe_temperature_us_state_no_labels(spark,'temperature_country_us_state.csv')

def create_dataframe_temperature_us_state(temperature_us_state_no_labels,label_us_state):
    
    # outer join temperature_us_state_no_labels to label_us_state by 
    # temperature_us_state_no_labels has more rows than label_us_state
    temperature_us_state = temperature_us_state_no_labels. \
        join(label_us_state, \
             [temperature_us_state_no_labels.state_name == label_us_state.state_name], \
             "left")
    
    # clean up
    temperature_us_state = temperature_us_state \
        .toDF('year','state_name','month','temperature','min_temperature','max_temperature','state_code','state_name_2') \
        .drop('state_name_2') \
        .na.drop(how="any")
    
    # create an unique id column
    temperature_us_state = temperature_us_state. \
        select('year','state_name','month','temperature','min_temperature','max_temperature','state_code', \
               F.row_number().over(Window.partitionBy().orderBy(['year','state_name','month'])).alias("id_temperature_us_state"))
    
#     # test
#     print("temperature_us_state")
#     print(temperature_us_state)
#     print(type(temperature_us_state))
#     temperature_us_state.show()
#     print(temperature_us_state.count())
    
    return temperature_us_state


def final_create_dataframe_temperature_us_state(spark,filePath):
    temperature_us_state_no_labels = create_dataframe_temperature_us_state_no_labels(spark,filePath)
    label_us_state = create_dataframe_label_us_state(spark)
    temperature_us_state = create_dataframe_temperature_us_state(temperature_us_state_no_labels,label_us_state)
    return temperature_us_state

###################################################################################################


def final_create_dataframe_I94s(spark,filePath, \
                                economics_country, \
                                economics_us_state, \
                                temperature_country, \
                                temperature_us_state \
                               ):
    
    I94s = spark.read.format('com.github.saurfang.sas.spark').load(filePath,forceLowercaseNames=True)

    # clean up and cast type  ; we drop the column 'count' and all the columns of type double are cast to Integer
    I94s = I94s.drop('count','visapost','occup','entdepu','insnum')\
        .withColumn("cicid",I94s["cicid"].cast(IntegerType())) \
        .withColumn("i94yr",I94s["i94yr"].cast(IntegerType())) \
        .withColumn("i94mon",I94s["i94mon"].cast(IntegerType())) \
        .withColumn("i94cit",I94s["i94cit"].cast(IntegerType())) \
        .withColumn("i94res",I94s["i94res"].cast(IntegerType())) \
        .withColumn("arrdate",I94s["arrdate"].cast(IntegerType())) \
        .withColumn("i94mode",I94s["i94mode"].cast(IntegerType())) \
        .withColumn("depdate",I94s["depdate"].cast(IntegerType())) \
        .withColumn("i94bir",I94s["i94bir"].cast(IntegerType())) \
        .withColumn("i94visa",I94s["i94visa"].cast(IntegerType())) \
        .withColumn("dtadfile",I94s["dtadfile"].cast(IntegerType())) \
        .withColumn("biryear",I94s["biryear"].cast(IntegerType())) \
        .withColumn("dtaddto",I94s["dtaddto"].cast(IntegerType())) \
        .withColumn("admnum",I94s["admnum"].cast(IntegerType())) \
        .withColumn("fltno",I94s["fltno"].cast(IntegerType())) \
        .dropDuplicates()
    

    # add the IDs columns from the 4 dataframes
    I94s = I94s.join(temperature_us_state, \
             [I94s.i94yr == temperature_us_state.year, \
              I94s.i94mon == temperature_us_state.month, \
              I94s.i94addr == temperature_us_state.state_code], \
             "left") \
            .join(temperature_country, \
             [I94s.i94yr == temperature_country.year, \
              I94s.i94mon == temperature_country.month, \
              I94s.i94res == temperature_country.country_code], \
             "left") \
            .join(economics_us_state, \
             [I94s.i94yr == economics_us_state.year, \
              I94s.i94mon == economics_us_state.month, \
              I94s.i94addr == economics_us_state.state_code], \
             "left") \
            .join(economics_country, \
             [I94s.i94yr == economics_country.year, \
              I94s.i94res == economics_country.country_code], \
             "left") \
             .select( "cicid","i94yr","i94mon","i94cit","i94res", \
                     "i94port","arrdate","i94mode","i94addr","depdate", \
                     "i94bir","i94visa","dtadfile","entdepa","entdepd", \
                     "matflag","biryear","dtaddto","gender","airline", \
                     "admnum","fltno","visatype", \
                     "id_temperature_us_state", \
                     "id_temperature_country", \
                     "id_economics_us_state", \
                     "id_economics_country" \
                    )
            
#     # test
#     I94s = I94s.na.drop(subset=["id_temperature_us_state","id_temperature_country","id_economics_us_state"])
    
#     # test
#     print("I94s")
#     print(I94s)
#     print(type(I94s))
#     I94s.show()
    
    return I94s




##################################################################################################

def main():    
    """
    - establishes a Spark session 
    - create the datframes economics_country , economics_us_state , temperature_country ,
        temperature_us_state. Use those tables to create the datframes I94s
    - write the dimension tables economics_country , economics_us_state , temperature_country ,
        temperature_us_state and fact table I94s
    
    """    
    
    spark = SparkSession.builder.\
    config("spark.jars.repositories", "https://repos.spark-packages.org/").\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
    appName('Pivot() Stack() PySpark'). \
    enableHiveSupport().getOrCreate()
    
    ##############create the datframes 
    economics_country = final_create_dataframe_economics_country(spark,'political_stability_country.csv', \
                                                                 'unemployment_country.csv', \
                                                                 'population_country.csv')
    economics_us_state = final_create_dataframe_economics_us_state(spark,'unemployment_us_state.csv')

    temperature_country = final_create_dataframe_temperature_country(spark,'temperature_country_us_state.csv')
    temperature_us_state = final_create_dataframe_temperature_us_state(spark,'temperature_country_us_state.csv')

    I94s = final_create_dataframe_I94s(spark,'../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat', \
                                       economics_country, \
                                       economics_us_state, \
                                       temperature_country, \
                                       temperature_us_state
                                      )

    ############# write the fact table I94s and the four dimension tables temperature_us_state, temperature_country, economics_us_state, economics_country

    I94s.write.parquet("fact_dimension_tables_02/I94s.parquet",partitionBy=['i94yr','i94mon'])

    temperature_us_state.write.parquet("fact_dimension_tables_02/temperature_us_state.parquet",partitionBy=['year','state_name'])
    temperature_country.write.parquet("fact_dimension_tables_02/temperature_country.parquet",partitionBy=['year','country_name'])

    economics_us_state.write.parquet("fact_dimension_tables_02/economics_us_state.parquet",partitionBy=['year','month'])
    economics_country.write.parquet("fact_dimension_tables_02/economics_country.parquet",partitionBy=['year'])


if __name__ == "__main__":
    main()
    