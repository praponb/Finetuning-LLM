from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter(F.col("store_id").isin([7, 12, 13, 25]))
    .select(
        "*",
        F.round((F.col("shipping_fee") * 1.07), 2).alias("shipping_fee_scaled"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("discount") >= 0.45, "PREMIUM").otherwise("STANDARD").alias("discount_tier"),
        F.when(F.col("amount") >= 1710, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("shipping_fee_scaled")).otherwise(0)).alias("shipped_shipping_fee_scaled"),
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)
