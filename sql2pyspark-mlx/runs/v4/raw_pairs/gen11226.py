from pyspark.sql import functions as F

result = (
    df_products
    .withColumn("quarter_start", F.date_trunc("quarter", "launch_date"))
    .withColumn("stock_qty_tier", F.when(F.col("stock_qty") >= 448, "PREMIUM").otherwise("STANDARD"))
    .rollup("quarter_start", "stock_qty_tier", "brand")
    .agg(
        F.sum(F.when(F.col("brand") == "Stark", F.col("stock_qty"))).alias("stark_total_stock_qty"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_rows"),
        F.countDistinct("brand").alias("distinct_brands"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("distinct_brands") > 1)
    .orderBy(F.col("quarter_start").desc_nulls_last())
)
