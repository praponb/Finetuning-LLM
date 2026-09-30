from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("order_id").orderBy(F.col("cost").desc())
w3 = Window.partitionBy("order_id").orderBy(F.col("cost_share").desc(), "id")

enriched = (
    df_shipments
    .select(
        "id",
        "order_id",
        "shipped_date",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.cume_dist().over(w2).alias("cost_cume_dist"),
    )
)

enriched_shipments = (
    enriched
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id").eqNullSafe(F.col("b.id")), "inner")
    .filter(F.col("b.shipping_fee") <= 20)
    .select("src.*", F.col("b.channel"), F.col("b.shipping_fee"))
)

latest = (
    enriched_shipments
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

base = (
    latest
    .filter(F.col("shipped_date") >= F.lit("2023-07-16").cast("date"))
    .withColumn("cost_share_per_shipping_fee", (F.col("cost_share") / F.nullif("shipping_fee", F.lit(0))))
    .withColumn("cost_share_times_shipping_fee", (F.col("cost_share") * F.col("shipping_fee")))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("OTHER")))
)

matched_keys = (
    base
    .select("order_id")
    .subtract(
        df_shipments
        .filter(F.col("carrier").isin(["UPS", "Flash"]))
        .select("order_id")
    )
)

verified = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("cost_share_times_shipping_fee") >= 83, "HIGH").otherwise("LOW").alias("cost_share_times_shipping_fee_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.round(F.avg("cost_share"), 2).alias("avg_cost_share"),
        F.percentile_approx("cost_share", 0.95).alias("cost_share_p95"),
    )
)
