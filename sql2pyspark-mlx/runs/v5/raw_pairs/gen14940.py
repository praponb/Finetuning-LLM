from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("shipping_fee_filled", "id")
w3 = Window.partitionBy("status").orderBy(F.col("running_count_shipping_fee").asc_nulls_last(), "id")
w_hist = Window.partitionBy("store_id").orderBy("running_count_shipping_fee", "id")

valid = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (~F.col("channel").eqNullSafe(F.lit("web"))))
    .withColumn("shipping_fee_filled", F.coalesce("shipping_fee", F.lit(0)))
)

scored = (
    valid
    .select(
        "*",
        F.avg("shipping_fee").over(w).alias("running_avg_shipping_fee"),
        F.max("shipping_fee").over(w).alias("running_max_shipping_fee"),
        F.min("shipping_fee").over(w).alias("running_min_shipping_fee"),
        F.count("shipping_fee").over(w).alias("running_count_shipping_fee"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

base = (
    ranked
    .filter(F.col("running_max_shipping_fee") >= 29)
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "amount",
        "discount",
        "shipping_fee",
        "shipping_fee_filled",
        "running_avg_shipping_fee",
        "running_max_shipping_fee",
        "running_min_shipping_fee",
        "running_count_shipping_fee",
        "event_seq",
        "row_num",
        F.when(F.col("shipping_fee") >= 36, "High").when(F.col("shipping_fee") >= 15, "Medium").otherwise("Low").alias("shipping_fee_tier"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

enriched = (
    base
    .select(
        "id",
        "store_id",
        "discount",
        "shipping_fee",
        "running_max_shipping_fee",
        "running_min_shipping_fee",
        "running_count_shipping_fee",
        "event_seq",
        "row_num",
        "shipping_fee_tier",
        "resolved_channel",
        F.lead("shipping_fee").over(w_hist).alias("next_shipping_fee"),
        F.lead("running_max_shipping_fee").over(w_hist).alias("next_running_max_shipping_fee"),
        F.lag("running_max_shipping_fee").over(w_hist).alias("prev_running_max_shipping_fee"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)

result = (
    enriched
    .alias("a")
    .join(df_stores.alias("dim"), F.col("a.store_id") == F.col("dim.id"), "inner")
    .rollup(F.col("dim.region").alias("region"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("a.next_shipping_fee")), 2).alias("total_next_shipping_fee"),
        F.min(F.col("a.discount")).alias("min_discount"),
    )
    .orderBy(F.col("min_discount").asc_nulls_first())
)
