from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "city")
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.count("sqft").alias("sqft_count"),
        F.avg("sqft").alias("avg_sqft"),
    )
    .filter(F.col("avg_sqft") < 11000)
    .drop("avg_sqft")
    .orderBy("sqft_count")
)
