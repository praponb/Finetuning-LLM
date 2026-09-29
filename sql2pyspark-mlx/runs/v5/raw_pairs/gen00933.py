from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date_period", "id")
w2 = Window.partitionBy("status").orderBy("order_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("status").orderBy("burst_id", "id").rowsBetween(-6, Window.currentRow)
amount_by_store_id = df_orders.groupBy("store_id").agg(F.avg("amount").alias("avg_order_amount"))

filtered = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-02-20").cast("date"))
    .select(
        "*",
        F.when(F.col("amount") >= 1420, "High").when(F.col("amount") >= 560, "Medium").otherwise("Low").alias("amount_tier"),
        F.date_trunc("YEAR", "order_date").alias("order_date_period"),
    )
)

flagged = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("order_date_period").over(w).isNull() | (F.col("order_date_period") > F.lag("order_date_period").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        "amount_tier",
        "order_date_period",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

metrics = (
    islands
    .select(
        "id",
        "store_id",
        "order_date",
        "channel",
        "discount",
        "shipping_fee",
        "amount_tier",
        "order_date_period",
        F.count("shipping_fee").over(w_grp).alias("last7_count_shipping_fee"),
        F.avg("shipping_fee").over(w_grp).alias("last7_avg_shipping_fee"),
        F.max("shipping_fee").over(w_grp).alias("last7_max_shipping_fee"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.quantity") >= 7), "left_semi")
)

wide = (
    eligible
    .groupBy("store_id")
    .pivot("channel", ["partner", "web"])
    .agg(F.sum("discount"))
)

result = (
    wide
    .join(amount_by_store_id, on="store_id", how="left")
    .select("store_id", "partner", "web", F.col("avg_order_amount"))
    .orderBy(F.col("avg_order_amount").asc_nulls_last(), "partner")
)
