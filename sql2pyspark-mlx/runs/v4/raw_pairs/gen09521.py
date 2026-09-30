from pyspark.sql import functions as F

result = (
    df_products
    .withColumn("year_start", F.date_trunc("year", "launch_date"))
    .withColumn("stock_qty_tier", F.when(F.col("stock_qty") >= 350, "LARGE").otherwise("SMALL"))
    .rollup("year_start", "stock_qty_tier")
    .agg(
        F.corr("stock_qty", "price").alias("corr_stock_qty_price"),
        F.stddev_samp("price").alias("price_stddev"),
        F.grouping_id().alias("gid"),
    )
)
