from pyspark.sql import functions as F

matched_keys = (
    df_order_items
    .select("order_id")
    .exceptAll(
        df_order_items
        .filter(F.col("product_id").isNull())
        .select("order_id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("product_id")
    .agg(
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.stddev_samp("unit_price").alias("unit_price_stddev_2"),
        F.sum("quantity").alias("total_quantity"),
    )
    .orderBy(F.col("total_quantity").desc_nulls_last())
)
