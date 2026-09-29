from pyspark.sql import functions as F

result = (
    df_products
    .withColumn("cost_tier", F.when(F.col("cost") >= 780, "LARGE").otherwise("SMALL"))
    .withColumn("week_start", F.date_trunc("WEEK", "launch_date"))
    .cube("cost_tier", "week_start")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.corr("stock_qty", "cost").alias("corr_stock_qty_cost"),
        F.percentile_approx("stock_qty", 0.95).alias("stock_qty_p95"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("corr_stock_qty_cost") >= 0)
)
