from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id")
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc())
w4 = Window.partitionBy("channel", "region").orderBy("discount", "id")

cleaned = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.date_add("order_date", 15).alias("order_date_plus_15d"),
        F.concat("channel", F.lit(" - "), "status").alias("channel_status"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.shipped_date") <= F.col("s.order_date")), "left_anti")
)

enriched = (
    kept
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.store_name"), F.col("p.region"))
)

filtered = (
    enriched
    .filter(F.col("customer_id").isNotNull() & (F.col("order_date") <= "2023-11-30"))
    .withColumn("days_order_date_plus_15d_to_order_date", F.datediff("order_date", "order_date_plus_15d"))
)

with_windows = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.min("days_order_date_plus_15d_to_order_date").over(w2).alias("status_min_days_order_date_plus_15d_to_order_date"),
        F.dense_rank().over(w3).alias("shipping_fee_dense_rank"),
        F.ntile(10).over(w4).alias("discount_bucket"),
    )
)

stats = (
    with_windows
    .groupBy(
        F.date_trunc("WEEK", "order_date_plus_15d").alias("week_start"),
        F.when(F.col("amount") >= 1590, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.count_if(F.col("channel") == "store").alias("store_events"),
        F.count_if(F.col("status") == "SHIPPED").alias("shipped_events"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
    )
)

result = (
    stats
    .select(
        "week_start",
        "amount_tier",
        "channel_list",
        "store_events",
        "status_list",
        "partner_count",
    )
)
