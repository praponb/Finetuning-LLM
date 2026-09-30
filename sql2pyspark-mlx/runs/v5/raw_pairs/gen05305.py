from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.avg("sqft").alias("avg_sqft"),
    )
    .filter(F.col("avg_sqft") >= 13100)
    .drop("avg_sqft")
)
