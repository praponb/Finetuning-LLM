from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.upper("store_name").like("%OWN%") | (F.col("opened_date") <= "2025-06-01"))
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"), "city")
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
    )
    .orderBy("city", F.col("total_sqft").desc())
)
