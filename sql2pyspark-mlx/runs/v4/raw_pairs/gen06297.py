from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("method")
w5 = Window.partitionBy("order_id", "method").orderBy(F.col("new_group_flag").desc(), "id")
w_recent = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("order_id").orderBy(F.col("last_known_paid_at_hour").desc_nulls_first(), "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.sum("cost").alias("sum_shipment_cost"))

base = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.date_format("paid_at", "yyyy-MM-dd").alias("paid_at_label"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_clean"),
    )
)

base_payments = (
    base
    .filter((F.col("paid_at") > F.lit("2023-12-08 04:00:00").cast("timestamp")) & (F.col("paid_at") >= "2025-05-17"))
    .select(
        "*",
        F.hour("paid_at").alias("paid_at_hour"),
        F.coalesce("method", F.lit("OTHER")).alias("resolved_method"),
    )
)

with_gaps = (
    base_payments
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

metrics = (
    sessions_payments
    .select(
        "*",
        F.max("amount").over(w_ordered).alias("amount_max_90d"),
        F.count("paid_at_hour").over(w4).alias("method_count_paid_at_hour"),
        F.last("new_group_flag", ignorenulls=True).over(w2).alias("last_known_new_group_flag"),
        F.ntile(10).over(w5).alias("new_group_flag_bucket"),
        F.last("paid_at_hour", ignorenulls=True).over(w_recent).alias("last_known_paid_at_hour"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
)

with_gaps_2 = (
    deduped
    .withColumn("new_group_flag_2", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = with_gaps_2.withColumn("session_id", F.sum("new_group_flag_2").over(w2))

with_dims = (
    islands
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.amount").alias("order_amount"), F.col("b.shipping_fee"))
)

result = (
    with_dims
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "paid_at_label",
        "method_clean",
        "paid_at_hour",
        "resolved_method",
        F.col("sum_shipment_cost"),
    )
    .orderBy(F.col("paid_at").desc())
)
