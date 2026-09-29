from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", F.quarter("launch_date").alias("launch_quarter"))
    .agg(
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Acme", 1).otherwise(0))) / F.count("*")), 1).alias("pct_acme"),
    )
)
