from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("cost") >= 670, "HIGH").otherwise("LOW").alias("cost_tier"),
        F.when(F.col("price") >= 1150, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
    )
    .agg(
        F.stddev_samp("price").alias("price_stddev"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.count_if(F.col("brand") == "Umbrella").alias("umbrella_events"),
    )
    .filter(F.col("stock_qty_bit_or") > 1)
)
