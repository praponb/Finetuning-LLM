from pyspark.sql import functions as F

matched_keys = (
    df_products
    .select("supplier_id", "brand")
    .intersectAll(
        df_products
        .filter(F.col("cost") <= 400)
        .select("supplier_id", "brand")
    )
)

reconciled = (
    df_products
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("supplier_id")
    .agg(
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.avg(F.when(F.col("brand") == "Stark", F.col("price"))).alias("stark_avg_price"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.stddev_samp("price").alias("price_stddev"),
    )
)
