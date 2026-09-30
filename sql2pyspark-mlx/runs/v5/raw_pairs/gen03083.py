from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("is_lost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_order_id = df_payments.groupBy("order_id").agg(F.min("amount").alias("min_payment_amount"))

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-04-09").cast("date")) & F.col("order_id").isNull())
    .select("*", F.when(F.col("status") == "LOST", 1).otherwise(0).alias("is_lost"))
)

enriched = (
    cleaned
    .select(
        "id",
        "order_id",
        "shipped_date",
        "status",
        F.sum("is_lost").over(w).alias("running_sum_is_lost"),
        F.count("is_lost").over(w).alias("running_count_is_lost"),
        F.max("is_lost").over(w).alias("running_max_is_lost"),
        F.sum("is_lost").over(w2).alias("cumulative_is_lost"),
    )
)

enriched_shipments = (
    enriched
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.shipped_date",
        "s.status",
        "s.cumulative_is_lost",
        F.col("p.shipping_fee"),
    )
)

joined = (
    enriched_shipments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.status",
        "src.shipping_fee",
        F.col("o.discount"),
    )
)

with_dims = (
    joined
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.amount"), F.col("p.discount").alias("order_discount"))
)

grouped = (
    with_dims
    .groupBy(
        "order_id",
        F.when(F.col("shipping_fee") >= 34, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_tier"),
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.percentile_approx("discount", 0.5).alias("discount_p50"),
        F.count_if(F.col("status") == "LOST").alias("lost_events"),
    )
    .filter(F.col("num_status") < 1000)
)

result = (
    grouped
    .join(amount_by_order_id, on="order_id", how="left")
    .select("order_id", "shipping_fee_tier", "num_status", F.col("min_payment_amount"))
)
