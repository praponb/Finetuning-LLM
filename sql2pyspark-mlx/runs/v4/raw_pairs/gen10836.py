from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Expre%"))
    .groupBy("region")
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.max("sqft").alias("max_sqft"),
    )
)
