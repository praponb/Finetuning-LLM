from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~(F.col("store_name").like("Si%") & F.col("store_name").like("%Expr%") & (F.col("opened_date") <= "2025-05-01")))
    .groupBy("city")
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bangkok"),
    )
)
