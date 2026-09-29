from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("store_id", "customer_id")
w_hist = Window.partitionBy("channel")
w4 = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").desc_nulls_first())
w_running = Window.partitionBy("channel", "store_id").orderBy(F.col("discount_per_shipping_fee").desc_nulls_last())

filtered = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        (F.col("discount") / F.nullif("shipping_fee", F.lit(0))).alias("discount_per_shipping_fee"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

matched_keys = (
    filtered
    .select("store_id", "id")
    .subtract(
        df_orders
        .filter(F.col("channel").isNull())
        .select("store_id", "id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .withColumn("customer_id_min_discount_per_shipping_fee", F.min("discount_per_shipping_fee").over(w))
    .withColumn("corr_shipping_fee_amount", F.corr("shipping_fee", "amount").over(w2))
    .withColumn("channel_avg_shipping_fee", F.avg("shipping_fee").over(w_hist))
    .withColumn("shipping_fee_dense_rank", F.dense_rank().over(w4))
    .withColumn("discount_per_shipping_fee_dense_rank", F.dense_rank().over(w_running))
)

enriched = (
    with_windows
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.opened_date"), F.col("d.sqft"))
)

result = (
    enriched
    .groupBy(F.date_trunc("YEAR", "opened_date").alias("year_start"))
    .agg(
        F.round(F.avg("discount_per_shipping_fee_dense_rank"), 2).alias("avg_discount_per_shipping_fee_dense_rank"),
        F.sum(F.when(F.col("channel") == "partner", F.col("amount")).otherwise(0)).alias("partner_amount"),
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_rows"),
        F.max("shipping_fee").alias("max_shipping_fee"),
    )
    .orderBy(F.col("max_shipping_fee").desc())
)
