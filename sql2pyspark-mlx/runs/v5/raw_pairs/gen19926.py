from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([1, 2, 4]) & (F.col("supplier_id").isNull() | (F.col("launch_date") >= "2025-02-01")))
    .groupBy(F.month("launch_date").alias("launch_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Initech", 1).otherwise(0))) / F.count("*")), 1).alias("pct_initech"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.round(((100 * F.sum(F.when(F.col("category") == "Sports", 1).otherwise(0))) / F.count("*")), 1).alias("pct_sports"),
    )
    .orderBy("pct_initech", "launch_month")
    .limit(5)
)
