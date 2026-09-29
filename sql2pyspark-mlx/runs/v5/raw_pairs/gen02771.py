from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(12370, 13250) & (F.col("sqft") < 7545))
    .withColumn("store_name_prefix", F.substring("store_name", 1, 2))
    .orderBy(F.col("sqft").desc(), "id")
    .limit(5)
)
