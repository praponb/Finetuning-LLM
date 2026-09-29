from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date_period", "id", "subscription_id")
w2 = Window.partitionBy("channel").orderBy("order_date_period", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_orders
    .filter(F.col("channel").isNotNull())
    .withColumn("order_date_period", F.date_trunc("WEEK", "order_date"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("N/A")))
)

with_window = (
    cleaned
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.order_date_period") >= F.col("sub.start_date")) & (F.col("src.order_date_period") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_gaps = (
    with_window
    .withColumn("is_new_burst", F.when(F.col("order_date_period") - F.lag("order_date_period").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_orders = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

daily = (
    sessions_orders
    .groupBy(F.date_trunc("YEAR", "order_date_period").alias("year_start"))
    .agg(
        ((F.unix_timestamp(F.max("order_date_period")) - F.unix_timestamp(F.min("order_date_period"))) / 60).alias("duration_min"),
        F.max("order_date_period").alias("ended_at"),
        F.percentile_approx("subscription_id", [0.25, 0.5, 0.75]).alias("subscription_id_quartiles"),
        F.avg(F.when(F.col("status") == "PENDING", F.col("shipping_fee"))).alias("pending_avg_shipping_fee"),
    )
    .filter(F.col("pending_avg_shipping_fee") >= 0)
)

result = (
    daily
    .select("year_start", "duration_min", "pending_avg_shipping_fee")
    .orderBy("pending_avg_shipping_fee")
)
