from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("shipping_fee").desc())
w2 = Window.partitionBy("status", "customer_id").orderBy(F.col("shipping_fee").desc_nulls_last())
w3 = Window.orderBy(F.col("discount").desc_nulls_first())

base = (
    df_orders
    .filter(F.col("channel").isin(["partner", "store", "mobile"]))
    .select(
        "*",
        F.coalesce("channel", F.lit("unassigned")).alias("channel_filled"),
        F.round((F.col("shipping_fee") / F.col("amount")), 2).alias("amount_with_shipping"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

kept_keys = (
    base
    .select("customer_id", "channel")
    .intersectAll(
        df_orders
        .filter(F.col("amount").between(430, 1140))
        .select("customer_id", "channel")
    )
)

reconciled = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "*",
        F.cume_dist().over(w).alias("shipping_fee_cume_dist"),
        F.dense_rank().over(w2).alias("shipping_fee_dense_rank"),
        F.rank().over(w3).alias("discount_rank"),
    )
)

enriched = (
    with_windows
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.email"), F.col("b.signup_date"))
)

result = (
    enriched
    .groupBy(
        "channel",
        F.when(F.col("shipping_fee_dense_rank") >= 9, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_dense_rank_tier"),
    )
    .agg(
        F.sum(F.when(F.col("channel") == "mobile", F.col("shipping_fee_dense_rank")).otherwise(0)).alias("mobile_shipping_fee_dense_rank"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("shipping_fee")).otherwise(0)).alias("completed_shipping_fee"),
    )
    .filter(F.col("distinct_customer_ids") < 1000)
    .orderBy(F.col("shipping_fee_dense_rank_tier").desc_nulls_first())
)
