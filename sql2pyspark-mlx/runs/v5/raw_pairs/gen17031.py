from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("shipping_fee", "id")
w_grp = Window.partitionBy("store_id").orderBy(F.col("amount_with_shipping").desc())
w_grp_2 = Window.partitionBy("store_id", "status").orderBy(F.col("discount").asc_nulls_last())
w5 = Window.partitionBy("status")

filtered = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-04-02").cast("date")) & F.col("store_id").isNotNull())
    .withColumn("status_upper", F.upper("status"))
)

enriched = (
    filtered
    .withColumn("discount_avg_6", F.avg("discount").over(w))
    .withColumn("next_discount", F.lead("discount", 2).over(w2))
)

kept_keys = (
    enriched
    .select("store_id", "channel")
    .exceptAll(
        df_orders
        .filter(F.col("amount") <= 1570)
        .select("store_id", "channel")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("status") == "SHIPPED")
    .withColumn("amount_with_shipping", (F.col("amount") + F.col("shipping_fee")))
)

metrics = (
    base
    .filter(F.col("next_discount") > 0.15)
    .select(
        "id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "status_upper",
        F.percent_rank().over(w_grp).alias("amount_with_shipping_percent_rank"),
        F.dense_rank().over(w_grp_2).alias("discount_dense_rank"),
        F.corr("shipping_fee", "discount").over(w5).alias("corr_shipping_fee_discount"),
    )
)

with_windows = (
    metrics
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w5)), 4))
)

eligible = (
    with_windows
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.order_id").isin([5, 6, 22])), "left_semi")
)

result = (
    eligible
    .cube("channel", "status")
    .agg(
        F.stddev_samp("discount").alias("discount_stddev"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
    .filter(F.col("discount_stddev") > 0)
    .orderBy(F.col("discount_stddev").asc_nulls_last())
)
