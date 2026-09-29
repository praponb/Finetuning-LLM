from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").desc())
w_ordered = Window.partitionBy("status", "channel")
w3 = Window.partitionBy("status").orderBy(F.col("shipping_fee_rounded").desc_nulls_last())
w4 = Window.partitionBy("store_id")
w_running = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc_nulls_first())
revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.max("revenue").alias("max_store_sale_revenue"))

cleaned = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-07-15").cast("date")) & (F.col("discount") < 0.31))
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "shipping_fee",
        F.round("shipping_fee", 2).alias("shipping_fee_rounded"),
        (F.col("shipping_fee") * F.col("amount")).alias("amount_with_shipping"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

matched_keys = (
    cleaned
    .select("status", "id")
    .intersect(
        df_orders
        .filter(F.col("discount") < 0.24)
        .select("status", "id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    kept_rows
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") < F.col("s.order_date")), "left_anti")
)

metrics = (
    eligible
    .select(
        "store_id",
        "status",
        "shipping_fee_rounded",
        F.cume_dist().over(w).alias("shipping_fee_cume_dist"),
        F.sum("amount_with_shipping").over(w_ordered).alias("status_sum_amount_with_shipping"),
        F.rank().over(w3).alias("shipping_fee_rounded_rank"),
        F.avg("shipping_fee_rounded").over(w4).alias("store_id_avg_shipping_fee_rounded"),
        F.cume_dist().over(w_running).alias("shipping_fee_cume_dist_2"),
    )
)

wide = (
    metrics
    .groupBy("store_id")
    .pivot("status", ["SHIPPED", "CANCELLED"])
    .agg(
        F.sum("shipping_fee_cume_dist_2").alias("value_sum"),
        F.count(F.lit(1)).alias("item_count"),
    )
    .select(
        "store_id",
        F.col("SHIPPED_value_sum").alias("shipped_value_sum"),
        F.col("SHIPPED_item_count").alias("shipped_item_count"),
        F.col("CANCELLED_value_sum").alias("cancelled_value_sum"),
        F.col("CANCELLED_item_count").alias("cancelled_item_count"),
    )
)

result = (
    wide
    .join(revenue_by_store_id, on="store_id", how="left")
    .select(
        "store_id",
        "shipped_value_sum",
        "shipped_item_count",
        "cancelled_item_count",
        F.col("max_store_sale_revenue"),
    )
    .orderBy(F.col("store_id").desc_nulls_last())
    .limit(20)
)
