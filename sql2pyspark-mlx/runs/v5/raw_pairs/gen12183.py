from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id", "channel").orderBy("discount", "id")
w2 = Window.partitionBy("customer_id").orderBy("prev_shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("next_amount_with_shipping", "id")
w4 = Window.partitionBy("status", "customer_id").orderBy("cumulative_amount_filled", "id")

filtered = (
    df_orders
    .filter(F.col("status") == "SHIPPED")
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        (F.col("amount") + F.col("shipping_fee")).alias("amount_with_shipping"),
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "customer_id",
        "status",
        "amount_filled",
        F.lag("shipping_fee").over(w_hist).alias("prev_shipping_fee"),
        F.lead("shipping_fee").over(w_hist).alias("next_shipping_fee"),
        F.lead("amount_with_shipping").over(w_hist).alias("next_amount_with_shipping"),
        F.lag("amount_with_shipping").over(w_hist).alias("prev_amount_with_shipping"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        (F.col("amount_filled") + F.col("next_amount_with_shipping")).alias("amount_filled_plus_next_amount_with_shipping"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.sum("amount_filled").over(w2).alias("cumulative_amount_filled"),
        F.lead("prev_shipping_fee", 1).over(w3).alias("next_prev_shipping_fee"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.month(F.col("ch.shipped_date")).isin([1, 2, 12])), "left_semi")
)

ranked = (
    kept
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .groupBy(
        "customer_id",
        F.when(F.col("amount_filled") >= 83, "LARGE").otherwise("SMALL").alias("amount_filled_tier"),
    )
    .agg(
        F.round(F.avg("next_prev_shipping_fee"), 2).alias("avg_next_prev_shipping_fee"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count_if(F.col("status") == "SHIPPED").alias("shipped_events"),
        F.round(F.sum("amount_filled") / F.nullif(F.count(F.when(F.col("status") == "SHIPPED", 1)), F.lit(0)), 2).alias("amount_filled_per_shipped"),
    )
    .orderBy(F.col("amount_filled_per_shipped").asc_nulls_first())
)
