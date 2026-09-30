from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles"),
        F.stddev_samp("price").alias("price_stddev"),
    )
    .filter(F.col("price_stddev") > 0)
)
