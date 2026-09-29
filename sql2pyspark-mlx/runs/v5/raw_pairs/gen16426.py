from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "status").orderBy(F.col("amount_with_shipping").desc_nulls_first(), "id")

base = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "store_id",
        "status",
        "channel",
        (F.col("amount") + F.col("shipping_fee")).alias("amount_with_shipping"),
        F.coalesce("shipping_fee", F.lit(0)).alias("shipping_fee_filled"),
    )
)

deduped = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "status", "channel", "amount_with_shipping", "shipping_fee_filled")
)

kept = (
    deduped
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.carrier").isin(["FedEx", "Kerry", "Flash"])), "left_semi")
)

cleaned = (
    kept
    .filter(F.col("shipping_fee_filled") < 21)
    .select(
        "*",
        F.when(F.col("channel") == "web", 1).otherwise(0).alias("is_web"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

totals = (
    cleaned
    .withColumn("amount_with_shipping_tier", F.when(F.col("amount_with_shipping") >= 78, "HIGH").otherwise("LOW"))
    .cube("amount_with_shipping_tier", "status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.sum("shipping_fee_filled") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("shipping_fee_filled_per_mobile"),
        F.count("*").alias("event_count"),
        F.grouping("amount_with_shipping_tier").alias("gid"),
    )
)

result = (
    totals
    .select(
        "amount_with_shipping_tier",
        "status",
        "status_list",
        "shipping_fee_filled_per_mobile",
        "event_count",
        "gid",
        F.when(F.col("status").rlike("^(PENDING|CANCELLED)$"), "CORE").otherwise("OTHER_ZONE").alias("status_zone"),
    )
)
