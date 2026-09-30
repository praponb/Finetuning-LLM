from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.upper("store_name").like("%ALL%") & F.col("store_name").like("%Siam%") & (F.col("city") == "London"))
    .groupBy(
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
        F.quarter("opened_date").alias("opened_quarter"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "London", F.col("sqft")).otherwise(0)).alias("london_sqft"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
    .orderBy(F.col("london_sqft").desc(), "opened_month", "opened_quarter")
    .limit(50)
)
