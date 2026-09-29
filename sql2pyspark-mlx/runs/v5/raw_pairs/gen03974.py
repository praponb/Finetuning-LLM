from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc_nulls_first())
w2 = Window.partitionBy("channel").orderBy(F.col("discount").desc_nulls_first())
w3 = Window.partitionBy("channel")

valid = (
    df_payments
    .filter(F.col("amount").isNull())
    .withColumn("amount_level", F.when(F.col("amount") >= 1510, "High").when(F.col("amount") >= 470, "Medium").otherwise("Low"))
)

matched_keys = (
    valid
    .select("order_id", "method")
    .intersectAll(
        df_payments
        .filter(F.col("amount") > 1030)
        .select("order_id", "method")
    )
)

verified = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .filter(F.col("method") != "bank_transfer")
    .select(
        "order_id",
        "method",
        "amount",
        F.cume_dist().over(w).alias("amount_cume_dist"),
    )
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id").eqNullSafe(F.col("p.id")), "left")
    .select("s.*", F.col("p.shipping_fee"), F.col("p.discount"), F.col("p.channel"))
)

valid_payments = (
    with_dims
    .filter(F.col("method").isNotNull() & (F.col("method").eqNullSafe(F.lit("paypal"))))
    .select(
        "method",
        "amount_cume_dist",
        "discount",
        "channel",
        F.round("amount", 1).alias("amount_rounded"),
    )
)

enriched = (
    valid_payments
    .select(
        "amount_cume_dist",
        "channel",
        F.percent_rank().over(w2).alias("discount_percent_rank"),
        F.avg("amount_cume_dist").over(w3).alias("channel_avg_amount_cume_dist"),
    )
)

totals = (
    enriched
    .rollup("channel")
    .agg(
        F.round(F.sum("amount_cume_dist") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("amount_cume_dist_per_store"),
        F.round(F.sum("discount_percent_rank") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("discount_percent_rank_per_partner"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
    )
    .filter(F.col("amount_cume_dist_per_store") > 0)
)

result = (
    totals
    .select("amount_cume_dist_per_store", "store_count")
    .orderBy(F.col("amount_cume_dist_per_store").desc_nulls_first(), "store_count")
)
