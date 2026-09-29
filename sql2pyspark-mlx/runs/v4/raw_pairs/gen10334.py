from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", F.month("opened_date").alias("opened_month"), "region")
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.count("sqft").alias("sqft_count"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
    )
    .filter(F.col("singapore_count") > 4)
    .drop("singapore_count")
    .orderBy(F.col("city").desc(), "region", "opened_month")
    .limit(3)
)
