from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("carrier").orderBy(F.col("shipping_fee").desc(), "id")
w5 = Window.partitionBy("order_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.shipping_fee"))
)

with_windows = (
    joined
    .select(
        "*",
        F.avg("cost").over(w).alias("cost_avg_2_prior"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
        F.sum("shipping_fee").over(w3).alias("running_shipping_fee"),
        F.ntile(4).over(w4).alias("shipping_fee_bucket"),
        F.sum("cost").over(w5).alias("cumulative_cost"),
    )
)

with_dims = (
    with_windows
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select(
        "e.*",
        F.col("o.shipping_fee").alias("order_shipping_fee"),
        F.col("o.status").alias("order_status"),
        F.col("o.order_date"),
    )
)

with_dims_shipments = (
    with_dims
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select(
        "src.*",
        F.col("d.channel"),
        F.col("d.shipping_fee").alias("order_shipping_fee_2"),
        F.col("d.order_date").alias("order_order_date"),
    )
)

stats = (
    with_dims_shipments
    .withColumn("week_start", F.date_trunc("WEEK", "shipped_date"))
    .withColumn("cost_avg_2_prior_tier", F.when(F.col("cost_avg_2_prior") >= 89, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["week_start", "cost_avg_2_prior_tier"], ["week_start"]],
        "week_start",
        "cost_avg_2_prior_tier",
    )
    .agg(
        F.count(F.when(F.col("order_status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.count(F.when(F.col("order_status") == "CANCELLED", 1)).alias("cancelled_rows"),
        F.sum(F.when(F.col("status") == "IN_TRANSIT", F.col("cost_avg_2_prior")).otherwise(0)).alias("in_transit_cost_avg_2_prior"),
        F.countDistinct("status").alias("distinct_status"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    stats
    .select(
        "week_start",
        "cancelled_count",
        "cancelled_rows",
        "in_transit_cost_avg_2_prior",
        "gid",
    )
    .orderBy(F.col("gid").desc(), F.col("cancelled_count").desc())
)
