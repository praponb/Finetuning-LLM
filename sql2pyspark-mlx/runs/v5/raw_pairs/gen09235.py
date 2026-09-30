from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").isNull() & F.col("sqft").isNotNull())
    .withColumn("region_lower", F.lower("region"))
)
