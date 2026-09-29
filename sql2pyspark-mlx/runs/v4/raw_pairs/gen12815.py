from pyspark.sql import functions as F

remaining_keys = (
    df_inventory
    .select("id")
    .intersect(
        df_inventory
        .filter(F.col("quantity").between(145, 148))
        .select("id")
    )
)

verified = (
    df_inventory
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("product_id")
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.sum("quantity").alias("total_quantity"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.bit_and("quantity").alias("quantity_bit_and"),
    )
)
