from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(
        F.sum(F.when(F.col("brand") == "Acme", 1).otherwise(0)).alias("acme_count"),
        F.round(((100 * F.sum(F.when(F.col("category") == "Electronics", 1).otherwise(0))) / F.count("*")), 1).alias("pct_electronics"),
    )
)
