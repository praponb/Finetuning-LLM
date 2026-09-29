from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "city")
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
        F.sum("sqft").alias("total_sqft"),
    )
    .filter(F.col("total_sqft") > 63000)
    .select("opened_month", "city", "bangkok_count", "total_sqft")
    .orderBy(F.col("total_sqft").desc(), "opened_month", "city")
    .limit(10)
)
