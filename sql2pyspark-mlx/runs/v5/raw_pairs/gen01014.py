from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.min("sqft").alias("min_sqft"),
    )
    .filter(F.col("min_sqft") >= 7400)
    .select("region", "latest_opened_date", "min_sqft")
)
