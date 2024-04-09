#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
from pyspark.conf import SparkConf
from pyspark.context import SparkContext


# In[4]:


spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-us-west1-488096447297-kpwrpixn')


# In[5]:


bucket = "staging-bucket-galvanic-crow-412709"
directory = "wa_state_ev_activity_data"
df = spark.read.option("basePath", "gs://{}/{}".format(bucket, directory)) \
    .parquet("gs://{}/{}".format(bucket, directory))


# In[7]:


df = df.drop(
    "vin_1_10",
    "electric_range",
    "odometer_reading",
    "odometer_code",
    "sale_price",
    "date_of_vehicle_sale",
    "base_msrp",
    "meets_2019_hb_2042_sale_price_value_requirement",
    "_2019_hb_2042_sale_price_value_requirement",
    "electric_vehicle_fee_paid",
    "transportation_electrification_fee_paid",
    "hybrid_vehicle_electrification_fee_paid",
    "census_tract_2020",
    "legislative_district",
    "electric_utility"
)   


# In[ ]:


# Determine the latest partition date
latest_partition_date = df.select(max(col("transaction_date"))).collect()[0][0]

# Filter the DataFrame to keep only the data from the latest partition
latest_partition_df = df.filter(col("transaction_date") == latest_partition_date)


# In[ ]:


latest_partition_df.createOrReplaceTempView("ev")

df_result = spark.sql("""
    SELECT transaction_date,
    transaction_year,
    transaction_type,
    electric_vehicle_type,
    model_year,
    make,
    model,
    vehicle_primary_use,
    new_or_used_vehicle,
    county,
    city,
    state_of_residence AS state,
    zip,
    COUNT(DISTINCT dol_vehicle_id) AS vehicle_registration_count
    FROM ev
    GROUP BY transaction_date,
    transaction_year,
    transaction_type,
    electric_vehicle_type,
    model_year,
    make,
    model,
    vehicle_primary_use,
    new_or_used_vehicle,
    county,
    city,
    state_of_residence,
    zip
""")


# In[ ]:


df_result.write \
    .format('bigquery') \
    .option('table', "wa_state_ev_activity_dataset.data") \
    .option('partitionField', 'transaction_date') \
    .option('partitionType', 'DAY') \
    .mode('append') \
    .save()

