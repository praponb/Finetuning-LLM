from pyspark.sql import functions as F

remaining_keys = (
    df_orders
    .select("status", "customer_id")
    .intersect(
        df_orders
        .filter(F.col("store_id").isNotNull())
        .select("status", "customer_id")
    )
)

verified = (
    df_orders
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("shipping_fee") >= 30, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_tier"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.corr("discount", "amount").alias("corr_discount_amount"),
        F.max(F.when(F.col("status") == "SHIPPED", F.col("discount"))).alias("shipped_max_discount"),
    )
    .filter(F.col("corr_discount_amount") > 0)
    .orderBy(F.col("shipped_max_discount").desc_nulls_first())
)
