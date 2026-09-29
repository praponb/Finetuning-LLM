from pyspark.sql import functions as F

valid_stores = (
    df_stores
    .filter(F.col("city").isin(["Tokyo", "New York"]))
    .withColumn("sqft_adjusted", F.round((F.col("sqft") * 0.9), 2))
)

summary = (
    valid_stores
    .groupBy(F.month("opened_date").alias("opened_month"), "region")
    .agg(F.avg("sqft").alias("avg_sqft"))
)

result = (
    summary
    .filter(F.col("avg_sqft") < 13200)
    .select("opened_month", "region", "avg_sqft")
)
