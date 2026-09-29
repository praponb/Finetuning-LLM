from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date_period", "id")
w2 = Window.partitionBy("channel").orderBy("order_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("order_date_period", "id")
w4 = Window.partitionBy("status").orderBy("order_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city").orderBy(F.col("amount").desc_nulls_first(), "id")

filtered = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.date_trunc("MONTH", "order_date").alias("order_date_period"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.lag("order_date_period").over(w).isNull() | (F.col("order_date_period") > F.lag("order_date_period").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_orders = gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

flagged = (
    sessions_orders
    .withColumn("session_start_flag", F.when(F.col("order_date_period") - F.lag("order_date_period").over(w3) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_orders_2 = flagged.withColumn("island_id", F.sum("session_start_flag").over(w4))

enriched = (
    sessions_orders_2
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.customer_id",
        "s.order_date",
        "s.status",
        "s.amount",
        "s.shipping_fee",
        "s.resolved_channel",
        "s.is_new_session",
        "s.session_start_flag",
        "s.island_id",
        F.col("p.email"),
        F.col("p.country"),
        F.col("p.city"),
    )
)

latest = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

filtered_orders = (
    latest
    .filter((~F.col("city").eqNullSafe(F.lit("Bangkok"))) & F.col("city").isNotNull())
    .select("*", F.last_day("order_date").alias("order_date_month_end"))
)

result = (
    filtered_orders
    .groupBy("customer_id")
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        F.percentile_approx("session_start_flag", [0.25, 0.5, 0.75]).alias("session_start_flag_quartiles"),
        F.stddev_samp("amount").alias("amount_stddev_2"),
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
        F.percentile_approx("island_id", 0.95).alias("island_id_p95"),
        F.max(F.when(F.col("status") == "RETURNED", F.col("shipping_fee"))).alias("returned_max_shipping_fee"),
    )
    .orderBy(F.col("amount_stddev_2").desc())
)
