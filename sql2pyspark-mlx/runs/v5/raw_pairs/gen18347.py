from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("status_length").desc_nulls_last())
w2 = Window.partitionBy("status", "method").orderBy(F.col("status_length").desc(), "id")
w3 = Window.partitionBy("order_id", "method").orderBy("paid_at", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w5 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status").orderBy("paid_at", "id")
w7 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("order_id").orderBy("status_length_bucket", "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

base = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-09-28").cast("date")) & (F.col("amount") <= 1200))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
        F.length("status").alias("status_length"),
        F.coalesce("method", F.lit("OTHER")).alias("resolved_method"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "days_since_paid_at",
        "status_length",
        "resolved_method",
        F.cume_dist().over(w).alias("status_length_cume_dist"),
        F.ntile(3).over(w2).alias("status_length_bucket"),
        F.avg("amount").over(w3).alias("amount_avg_2_prior"),
    )
)

gaps = (
    with_windows
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w4).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w4) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_session").over(w5))

gaps_2 = (
    islands
    .withColumn("is_new_session_2", F.when(F.col("paid_at") - F.lag("paid_at").over(w6) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands_payments = gaps_2.withColumn("session_id", F.sum("is_new_session_2").over(w7))

joined = (
    islands_payments
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.channel") != "partner")
    .select("src.*", F.col("b.discount"), F.col("b.order_date"))
)

base_payments = (
    joined
    .filter(F.col("order_id").isNotNull() & (F.col("paid_at") > F.lit("2023-09-16 07:00:00").cast("timestamp")))
    .select("*", F.date_trunc("week", "paid_at").alias("paid_at_period"))
)

latest = (
    base_payments
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        "resolved_method",
        "status_length_cume_dist",
        "status_length_bucket",
        "is_new_session",
        "session_no",
        F.when(F.col("status").rlike("^(SUCCESS|FAILED)$"), "GROUP_B").when(F.col("status").rlike("^(REFUNDED)$"), "CORE").otherwise("PRIORITY").alias("status_zone"),
        F.col("max_shipment_cost"),
    )
    .orderBy(F.col("is_new_session").desc_nulls_first())
)
