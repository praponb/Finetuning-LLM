from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") <= "2023-09-01")
    .groupBy("city")
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.count("sqft").alias("sqft_count"),
    )
    .filter(F.col("sqft_count") > 3)
)
