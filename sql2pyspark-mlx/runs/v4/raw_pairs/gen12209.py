from pyspark.sql import functions as F

remaining_keys = (
    df_products
    .select("brand")
    .subtract(
        df_products
        .filter(F.col("brand") != "Acme")
        .select("brand")
    )
)

reconciled = (
    df_products
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("supplier_id")
    .agg(
        F.stddev_samp("price").alias("price_stddev"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.size(F.collect_set("category")).alias("num_categories"),
        F.count_if(F.col("category") == "Fashion").alias("fashion_events"),
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles"),
    )
    .orderBy(F.col("fashion_events").desc_nulls_first())
)
