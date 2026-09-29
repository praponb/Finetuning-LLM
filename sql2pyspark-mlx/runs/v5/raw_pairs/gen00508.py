from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("paid_at", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("status", "method")
w5 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w6 = Window.partitionBy("order_id", "status")
w_recent = Window.partitionBy("order_id").orderBy(F.col("running_amount").desc())
w8 = Window.orderBy("amount", "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.sum("cost").alias("sum_shipment_cost"))

metrics = (
    df_payments
    .withColumn("running_amount", F.sum("amount").over(w))
    .withColumn("running_amount_2", F.sum("amount").over(w2))
    .withColumn("amount_avg_3_prior", F.avg("amount").over(w3))
    .withColumn("status_count_amount", F.count("amount").over(w4))
    .withColumn("next_amount", F.lead("amount", 1).over(w5))
)

with_windows = (
    metrics
    .select(
        "*",
        F.round((F.col("running_amount") / F.sum("running_amount").over(w6)), 4).alias("running_amount_share"),
        F.percent_rank().over(w_recent).alias("running_amount_percent_rank"),
        F.lag("running_amount_2", 1).over(w8).alias("prev_running_amount_2"),
    )
)

base = (
    with_windows
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "running_amount",
        "amount_avg_3_prior",
        "status_count_amount",
        "next_amount",
        "running_amount_share",
        "running_amount_percent_rank",
        "prev_running_amount_2",
        F.coalesce("prev_running_amount_2", F.lit(0)).alias("prev_running_amount_2_filled"),
    )
)

with_dims = (
    base
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id").eqNullSafe(F.col("o.id")), "left")
    .filter(F.col("o.amount") < 720)
    .select("a.*", F.col("o.order_date"), F.col("o.status").alias("order_status"))
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
        "status",
        "running_amount",
        "amount_avg_3_prior",
        "status_count_amount",
        F.when(F.col("status").rlike("^(SUCCESS|REFUNDED)$"), "EXTENDED").when(F.col("status").rlike("^(FAILED)$"), "GROUP_A").otherwise("PRIORITY").alias("status_zone"),
        F.col("sum_shipment_cost"),
    )
)
