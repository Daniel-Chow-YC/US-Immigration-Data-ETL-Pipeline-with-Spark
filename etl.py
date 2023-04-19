import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, year, month, col, round, dayofweek, weekofyear, dayofmonth
from pyspark.sql.types import StringType
import re
import configparser
from pathlib import Path

# Read config
config = configparser.ConfigParser()
config.read('config.cfg')

def main()-> None:
    spark = get_spark_session()
    
    # Set output path
    OUTPUT_DATA_DIR = config['DATA']['OUTPUT_DATA_DIR']
    
    # Load data from data sources into spark dataframes
    df_immigration = get_immigration_data(spark, config['DATA']['IMMIGRATION_DATA_FILE_PATH'])
    df_temperature = get_temperature_data(spark, config['DATA']['TEMPERATURE_DATA_FILE_PATH'])
    df_demo = get_demographics_data(spark, config['DATA']['DEMOGRAPHICS_DATA_FILE_PATH'])
    
    # Clean DataSets
    df_immigration_clean = clean_immigration_data(df_immigration)
    df_temperature_clean = clean_temperature_data(df_temperature)
    df_demo_clean = clean_demographics_data(df_demo)
    
    # Load staging tables
    df_staging_demographics = load_staging_demo(df_demo_clean)
    df_staging_temperature = load_staging_temperature(df_temperature_clean)
    df_staging_immigration = load_staging_immigration(df_immigration_clean)

    # create dimension and fact tables
    df_dim_immigrant = create_dim_immigrant_table(df_staging_immigration)
    df_dim_city = create_dim_city_table(df_staging_demographics, df_staging_temperature)
    df_dim_city_temp = create_dim_city_temp_table(df_staging_temperature)
    df_dim_time = create_dim_time_table(df_staging_immigration)
    df_fact_immigration = create_fact_immigration_table(df_staging_immigration)
    
    # Run data quality checks
    dfs_dict = {"df_dim_immigrant": df_dim_immigrant,
            "df_dim_city": df_dim_city, 
            "df_dim_city_temp": df_dim_city_temp, 
            "df_dim_time": df_dim_time, 
            "df_fact_immigration": df_fact_immigration}
    
    print(run_checks(dfs_dict))
    
    # Save table into parquet files
    Path(OUTPUT_DATA_DIR).mkdir(parents=True, exist_ok=True)
    
    df_dim_immigrant.write.mode("overwrite").parquet(OUTPUT_DATA_DIR + "immigrant")
    df_dim_city.write.mode("overwrite").parquet(OUTPUT_DATA_DIR + "city")
    df_dim_city_temp.write.mode("overwrite").parquet(OUTPUT_DATA_DIR + "city_temperature")
    df_dim_time.write.mode("overwrite").parquet(OUTPUT_DATA_DIR + "time")
    df_fact_immigration.write.mode("overwrite").parquet(OUTPUT_DATA_DIR + "immigration")
    
def run_checks(dfs_dict):
    result = 0
    total = 0
    for df_name, df in dfs_dict.items():
        result += table_exists(df, df_name) + records_found(df, df_name)
        total  += 2
    
    if result == total:
        msg = f"Checks complete. {result}/{total} checks passed. All checks passed!"
    else:
        msg = f"Checks complete. {result}/{total} checks passed. {total-result} checks failed!"
    return msg

# Check Tables exist
def table_exists(df, df_name):
    try:
        if df is not None:
            print(f"Table existance check passed. {df_name} found!")
            return 1
        else:
            print(f"Table existance check failed. {df_name} could not be found!")
            return 0
        
    except Exception as e:
        
        print((f"Table existance check failed for {df_name}. Error occurred during check: {e}"))
        return 0

# Check number of records in tables
def records_found(df, df_name):
    
    try:
        
        record_count = df.count()
    
        if record_count == 0:
            print(f"Record check failed for {df_name}. No records found!")
            return 0
        else:
            print (f"Record check passed for {df_name}. {record_count} records found!")
            return 1
    
    except Exception as e:
        
        print((f"Record check failed for {df_name}. Error occurred during check: {e}"))
        return 0


def create_fact_immigration_table(df_staging_immigration):
    df_immigration = df_staging_immigration.select("id", "state_code", "city_code", "date", "count").drop_duplicates()
    return df_immigration

def create_dim_time_table(df_staging_immigration):
    df_time = df_staging_immigration.withColumn("weekday", dayofweek("date"))\
                .withColumn("day", dayofmonth("date"))\
                .withColumn("week", weekofyear("date"))\
                .withColumn("month", month("date"))\
                .withColumn("year", year("date"))
    
    
    df_time = df_time.select("date", "day", "month", "year", "weekday", "week").drop_duplicates()
    
    return df_time

def create_dim_city_temp_table(df_staging_temperature):
    df_city_temp = df_staging_temperature.select("date", "city_code", "year", "month",
                                             "avg_temperature","avg_temperature_uncertainty").drop_duplicates()
    
    return df_city_temp

def create_dim_city_table(df_staging_demographics, df_staging_temperature):
    df_city = df_staging_demographics.join(df_staging_temperature, "city_code").select(
                                    "city_code", "state_code", df_staging_demographics["city_name"],
                                    "state_name",
                                    "median_age","male_pop",
                                    "female_pop","total_pop",
                                    "veterans_count","foreign_born_count"
                                    ,"avg_household_size","american_indian_alaska_native_count",
                                    "asian_count", "black_or_african_american_count",
                                    "hispanic_or_latino_count",
                                    "white_count","lat","long").drop_duplicates()
    return df_city
    
def create_dim_immigrant_table(df_staging_immigration):
    
    df_immigrant = df_staging_immigration.select("id", "age", "gender", "visa_type")
    
    return df_immigrant
    
def load_staging_demo(df_demo_clean):
    
    df_staging_demographics = df_demo_clean.select(
                                        col("City").alias("city_name"),
                                        col("city_code"),
                                        col("state").alias("state_name"),
                                        col("State Code").alias("state_code"),
                                        round(col("Median Age"), 2).alias("median_age"),
                                        col("Male Population").alias("male_pop"), 
                                        col("Female Population").alias("female_pop"),
                                        col("Total Population").alias("total_pop"),
                                        col("Number of Veterans").alias("veterans_count"),
                                        col("Foreign-born").alias("foreign_born_count"),
                                        round(col("Average Household Size"),2).alias("avg_household_size"),
                                        col("American Indian and Alaska Native").alias("american_indian_alaska_native_count"),
                                        col("Asian").alias("asian_count"),
                                        col("Black or African-American").alias("black_or_african_american_count"),
                                        col("Hispanic or Latino").alias("hispanic_or_latino_count"),
                                        col("White").alias("white_count"),
                                        ).drop_duplicates()
    
    return df_staging_demographics
    
def load_staging_temperature(df_temperature_clean):
    
    df_staging_temperature = df_temperature_clean.select(
                                        col('dt').alias("date"),
                                        col("year"), 
                                        col("month"), 
                                        col("city_code"),
                                        col("City").alias("city_name"),
                                        round(col("AverageTemperature"), 2).alias("avg_temperature"),
                                        round(col("AverageTemperatureUncertainty"), 2).alias("avg_temperature_uncertainty"),
                                        col("Latitude").alias("lat"), 
                                        col("Longitude").alias("long")
                                        ).drop_duplicates()
    
    return df_staging_temperature
    

def load_staging_immigration(df_immigration_clean):
    
    df_staging_immigration = df_immigration_clean.select(
                            col("cicid").alias("id"),
                            col("arrdate").alias("date"),
                            col("i94port").alias("city_code"),
                            col("i94addr").alias("state_code"),
                            col("i94bir").alias("age"),
                            col("gender"),
                            col("i94visa").alias("visa_type"),
                            col("count")
                            ).drop_duplicates()
    
    return df_staging_immigration

def get_spark_session():
    spark = SparkSession.builder.\
    config("spark.jars.repositories", "https://repos.spark-packages.org/").\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
    enableHiveSupport().getOrCreate()
    return spark

# functions to clean data
def clean_immigration_data(df_immigration):
    
    # remove duplicates
    df_immigration_clean = df_immigration.drop_duplicates()
    
    # convert SAS date to date 
    df_immigration_clean = df_immigration_clean.withColumn("arrdate",convert_to_date(df_immigration_clean.arrdate))
    df_immigration_clean = df_immigration_clean.withColumn("depdate",convert_to_date(df_immigration_clean.depdate))
    
    return df_immigration_clean

def clean_temperature_data(df_temperature):
    
    # keep only U.S. data
    df_temperature_clean = df_temperature.where(df_temperature.Country == 'United States')
    
    # drop any columns with missing average temperature value
    df_temperature_clean = df_temperature_clean.dropna(how="any", subset=["AverageTemperature"])
    
    # map city codes and drop any blanks
    df_temperature_clean = df_temperature_clean.withColumn("city_code", city_to_city_code(df_temperature_clean["City"]))
    df_temperature_clean = df_temperature_clean.dropna(how='any', subset=["city_code"])

    # correct data types
    df_temperature_clean = df_temperature_clean.withColumn("AverageTemperature", col("AverageTemperature").cast("float")) 
    df_temperature_clean = df_temperature_clean.withColumn("AverageTemperatureUncertainty", col("AverageTemperatureUncertainty").cast("float")) 

    # add year and month columns
    df_temperature_clean = df_temperature_clean.withColumn("year", year(df_temperature_clean['dt'])) \
                            .withColumn("month", month(df_temperature_clean["dt"]))
 
    return df_temperature_clean

def clean_demographics_data(df_demo):
    
    # map city codes and drop any blanks
    df_demo_clean = df_demo.withColumn("city_code", city_to_city_code(df_demo["City"]))
    df_demo_clean = df_demo_clean.dropna(how='any', subset=["city_code"])
    
    # correct data types - convert strings to long or float values
    df_demo_clean = df_demo_clean.withColumn("Count", col("Count").cast("long")) 
    df_demo_clean = df_demo_clean.withColumn("Median Age", col("Median Age").cast("float")) 
    df_demo_clean = df_demo_clean.withColumn("Male Population", col("Male Population").cast("long")) 
    df_demo_clean = df_demo_clean.withColumn("Female Population", col("Female Population").cast("long")) 
    df_demo_clean = df_demo_clean.withColumn("Number of Veterans", col("Number of Veterans").cast("long")) 
    df_demo_clean = df_demo_clean.withColumn("Total Population", col("Total Population").cast("long")) 
    df_demo_clean = df_demo_clean.withColumn("Foreign-born", col("Foreign-born").cast("long")) 
    df_demo_clean = df_demo_clean.withColumn("Average Household Size", col("Average Household Size").cast("float")) 
    
    df_demo_clean = df_demo_clean.groupBy(['City','city_code','State', 'State Code','Median Age','Male Population','Female Population','Total Population','Number of Veterans','Foreign-born','Average Household Size']).pivot('Race').sum('Count')
    
    return df_demo_clean

# Create dictionary of city codes: city name
def get_city_codes_from_sas_labels_file(i94_sas_label_descriptions_fname):

    with open(i94_sas_label_descriptions_fname) as f:
        lines = f.readlines()

    pattern_object = re.compile(r"\'(.*)\'.*\'(.*)\'")
    city_dict = {}
    for line in lines[302:961]:
        city_code_map = pattern_object.search(line)
        city_dict[city_code_map.group(1)] = city_code_map.group(2).rstrip()
        
    return city_dict

# function to determine city code from city name
@udf(StringType())
def city_to_city_code(city):
    city_dict = get_city_codes_from_sas_labels_file(config['DATA']['SAS_LABELS_DESCRIPTION_FILE_PATH'])
    for key in city_dict:
        if city.lower() in city_dict[key].lower():
            return key

# Create udf to convert SAS date value to date     
@udf(StringType())
def convert_to_date(x):
    if x:
        return ((datetime(1960, 1, 1).date() + timedelta(x))).isoformat()
    return None

# functions to get source data 
def get_immigration_data(spark: SparkSession, fname: str) -> DataFrame:
    return spark.read.format('com.github.saurfang.sas.spark').load(fname)

def get_temperature_data(spark: SparkSession, fname: str) -> DataFrame:
    return spark.read.format("csv").option("delimiter", ",").option("header", "true").load(fname)

def get_demographics_data(spark: SparkSession, fname: str) -> DataFrame:
    return spark.read.format("csv").option("delimiter", ";").option("header", "true").load(fname)

if __name__ == '__main__':
    main()