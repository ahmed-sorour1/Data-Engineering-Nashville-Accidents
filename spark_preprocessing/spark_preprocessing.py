#!/usr/bin/env python
# coding: utf-8
import numpy as np
import pyspark
import pyspark.sql.functions as fn
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import pandas as pd
from IPython.display import display, HTML


display(HTML("<style>pre {white-space: pre !important; }</style>"))

spark = SparkSession.builder.getOrCreate()
df = spark.read.format('csv')\
                .option('header', 'True')\
                .option('inferschema', 'True')\
                .load('Nashville Accidents Jan 2018 - Apl 2025.csv')



#### shows number of null values at each column:

def nullcol(df):
    df_null_cols = df.select([sum(col(c).isNull().cast('int')).alias(c) for c in df.columns])
    df_null_cols.show()



### Get the NUMBER of distinct values for each column:

df_distinct_cols = df.select([
                          countDistinct(c).alias(c) 
                          for c in df.columns])

### Get the distinct values for each column:

from pyspark.sql import Row
unique_values_dict = {}
max_len = 11

for col_name in df.columns:
    values = df.select(col_name).distinct().limit(max_len).rdd.flatMap(lambda x: x).collect()
    padded_values = values + [None] * (max_len - len(values))
    unique_values_dict[col_name] = padded_values
rows = [Row(**{col: unique_values_dict[col][i] for col in df.columns}) for i in range(max_len)]


spark_df_distinct = spark.createDataFrame(rows)

spark_df_distinct.show(truncate=False)


### Filling null with Median:
##### (Number of Motor Vehicles, Number of injuries, Number of Fatalities)

median_motor_vecheils = df.select(fn.median(col('Number of Motor Vehicles'))).collect()
median_Number_of_injuries = df.select(fn.median(col('Number of Injuries'))).collect()
median_Number_of_Fatalities = df.select(fn.median(col('Number of Fatalities'))).collect()

median_dict = {
    "Number of Motor Vehicles": median_motor_vecheils[0][0],
    "Number of Injuries": median_Number_of_injuries[0][0],
    "Number of Fatalities": median_Number_of_Fatalities[0][0]
}

df_fill_na = df.na.fill(median_dict)


### Filling Null With "UNKNOWN":

df_fill_na_cat = df_fill_na.fillna({"Collision Type Description": "UNKNOWN",
                         "Weather Description":"UNKNOWN",
                         "Illumination Description":"UNKNOWN",
                          "Street Address": "UNKNOWN",
                        "HarmfulCodes":"UNKNOWN",
                         "HarmfulDescriptions":"UNKNOWN",
                        })


### Drop Nulls:

columns_to_drop = ["Weather", "IlluACCIDEmination","Collision Type"]
df_drop_null_columns = df_fill_na_cat.drop(*columns_to_drop)


#df_drop_null_columns.groupBy("Collision Type Description", "Property Damage").count().orderBy("Collision Type Description").show(30,truncate=False)


#df.select(df['Property Damage']).where(col('Property Damage') == 'Y').count()


#df.select(df['Property Damage']).where(col('Property Damage') == 'N').count()




### Handeling Nulls in Property Damage inrespective to "Collision Type Description":


df_drop_null_columns.createOrReplaceTempView("PropertyDamage")
df_handle_property_damage = spark.sql("""
SELECT 
    *,
    CASE 
        WHEN `Property Damage` IS NOT NULL THEN `Property Damage`
        WHEN `Collision Type Description` IN (
            'REAR-TO-REAR', 'Front to Rear', 'HEAD-ON', 
            'ANGLE', 'Rear to Side', 'NOT COLLISION W/MOTOR VEHICLE-TRANSPORT'
        ) THEN 'Yes'
        WHEN `Collision Type Description` = 'SIDESWIPE - OPPOSITE DIRECTION' THEN 'Possible'
        ELSE 'No'
    END AS `Property Damage Imputed`
FROM PropertyDamage
""")


df_drop_property_damage = df_handle_property_damage.drop("Property Damage")


df_rename_property_damage = df_drop_property_damage.withColumnRenamed("Property Damage Imputed", "Property Damage")



df_yes_no_property_damage = df_rename_property_damage.withColumn(
    "Property Damage",
    when(col("Property Damage") == "Y", "Yes")
    .when(col("Property Damage") == "N", "No")
    .otherwise(col("Property Damage")))



df_cleaned = df_yes_no_property_damage.dropna()




### Extract DateTime data:

df2 = df_cleaned.withColumn("PM/AM", fn.substring(col("Date and Time"),-2,2))

df2 = df2.withColumn("Date and Time", to_timestamp("Date and Time", "M/d/yyyy h:mm:ss a"))

df_final = df2.withColumn("year", year("Date and Time")) \
       .withColumn("month", month("Date and Time"))\
       .withColumn("day", dayofmonth("Date and Time"))\
       .withColumn("Time", date_format("Date and Time", "HH:mm:ss"))\
       .withColumn("hour", hour("Date and Time")) \
       .withColumn("minutes", minute("Date and Time"))

df_final = df_final.select(['Accident Number',
 'Date and Time',
 'year',
 'month',
 'day',
 'Time',
 'hour',
 'minutes',
 'PM/AM',
 'Number of Motor Vehicles',
 'Number of Injuries',
 'Number of Fatalities',
 'Property Damage',
 'Hit and Run',
 'Collision Type Description',
 'Weather Description',
 'Illumination Description',
 'Street Address',
 'City',
 'State',
 'Precinct',
 'Lat',
 'Long',
 'HarmfulCodes',
 'HarmfulDescriptions',
 'ObjectId',
 'Zip Code',
 'RPA',
 'Reporting Officer',
 'x',
 'y'
 ])



### Handle the comma ',' in Columns:


string_columns = [field.name for field in df_final.schema.fields if isinstance(field.dataType, StringType)]

for column in string_columns:
    df_final = df_final.withColumn(column, regexp_replace(col(column), ",", ";"))

df_final.coalesce(1).write.mode("overwrite").option("header", True).csv("nashville_accidents.csv")