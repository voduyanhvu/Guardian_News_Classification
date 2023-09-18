import streamlit as st
import pymongo
import pandas as pd

# Spark SQL to processs with data frame
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

newDF = [
    StructField("id", IntegerType(), True),
    StructField("text", StringType(), True),
    StructField("Prediction", DoubleType(), True),
    StructField("Label", DoubleType(), True)]
finalSchema = StructType(fields=newDF)
news_df = spark.read.format('csv').options(header='true',schema=finalSchema,delimiter='|').load('output/streamlog.csv')

news_df=news_df.withColumn("Prediction", news_df["Prediction"].cast("Integer"))
news_df=news_df.withColumn("Label", news_df["Label"].cast("Integer"))

newDF = [
    StructField("Section", StringType(), True),
    StructField("Label", IntegerType(), True)]
finalSchema = StructType(fields=newDF)
labels_df = spark.read.format('csv').options(header='false',schema=finalSchema,delimiter=',').load('data/labels.csv')
labels_df = labels_df.withColumnRenamed("_c0","Section")
labels_df = labels_df.withColumnRenamed("_c1","Prediction")

df = news_df.drop(news_df['Label']).join(labels_df,"Prediction")
df = df.drop(df["Prediction"])

# group by Section and see the distribution
chart_df = df.groupBy("Section").count().sort("Section", ascending=False)

# Streamlit explorationary data analysis
# Use the full page instead of a narrow central column
st.set_page_config(layout="wide")
st.title("Guardian News Classifier")

# Display a chart showing the number of articles section-wise
st.bar_chart(chart_df, x='Section', use_container_width=True)

# List of sections
st.sidebar.title("Section")
pd = labels_df.toPandas()
menu = list(pd["Section"])
choice = st.sidebar.selectbox("Select a section", menu)

# Return the results
if choice != "":
    result_df = df.filter(df["Section"] == choice)
    st.table(result_df)



