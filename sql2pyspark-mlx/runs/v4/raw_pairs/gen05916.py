from pyspark.sql import functions as F

result = (
    df_products
    .withColumn("activity_month", F.date_trunc("MONTH", "launch_date"))
    .rollup("activity_month", "supplier_id")
    .agg(
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.count("*").alias("cnt"),
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
        F.grouping("activity_month").alias("grouping_id"),
    )
)
