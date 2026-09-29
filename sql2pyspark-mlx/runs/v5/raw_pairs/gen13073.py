from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id", "status").orderBy(F.col("paid_at").desc_nulls_last(), "id")
w4 = Window.partitionBy("order_id", "method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.partitionBy("method", "status").orderBy("paid_at", "id")
w6 = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("method").orderBy(F.col("row_num").desc(), "id")
w8 = Window.partitionBy("status")

with_dims = (
    df_payments
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.amount").alias("order_amount"))
)

with_dims_payments = (
    with_dims
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.discount"), F.col("b.shipping_fee"))
)

with_gaps = (
    with_dims_payments
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

latest = (
    islands
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

metrics = (
    latest
    .withColumn("is_new_session_max_7d", F.max("is_new_session").over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("first_known_discount", F.first("discount", ignorenulls=True).over(w6))
    .withColumn("seq_no_2", F.row_number().over(w7))
    .withColumn("status_avg_order_amount", F.avg("order_amount").over(w8))
)

base = (
    metrics
    .filter(F.col("paid_at") > F.lit("2023-05-30 14:00:00").cast("timestamp"))
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        "order_amount",
        "shipping_fee",
        "is_new_session",
        "session_no",
        "row_num",
        "is_new_session_max_7d",
        "first_known_discount",
        "status_avg_order_amount",
        F.coalesce("status_avg_order_amount", F.lit(0)).alias("status_avg_order_amount_filled"),
    )
)

result = (
    base
    .select(
        "id",
        "amount",
        "status",
        "shipping_fee",
        "is_new_session",
        "session_no",
        "row_num",
        "status_avg_order_amount",
        "status_avg_order_amount_filled",
        F.when(F.col("status").rlike("^(SUCCESS|FAILED)$"), "GROUP_A").when(F.col("status").rlike("^(REFUNDED)$"), "EXTENDED").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy("is_new_session")
)
