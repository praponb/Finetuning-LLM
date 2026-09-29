from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isNotNull())
    .groupBy(
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
        "region",
        "city",
    )
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("sqft")).otherwise(0)).alias("singapore_sqft"),
        F.sum("sqft").alias("total_sqft"),
    )
    .filter(F.col("total_sqft") > 32000)
    .orderBy("total_sqft")
)
