from pyspark.sql import functions as F

base = (
    df_stores
    .filter((F.col("region") == "South") & (F.col("opened_date") < "2025-01-01"))
)

city_summary = (
    base
    .groupBy("city", F.year("opened_date").alias("opened_year"))
    .agg(
        F.sum(F.when(F.col("city") == "London", F.col("sqft")).otherwise(0)).alias("london_sqft"),
    )
)

result = (
    city_summary
    .filter(F.col("london_sqft") >= 73)
    .orderBy("opened_year")
)
