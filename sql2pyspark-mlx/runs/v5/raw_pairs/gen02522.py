from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("paid_at").asc_nulls_last())
w2 = Window.partitionBy("status").orderBy("amount", "id")

base = (
    df_payments
    .filter(F.col("method") != "card")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.when(F.col("status") == "FAILED", 1).otherwise(0).alias("is_failed"),
    )
)

enriched = (
    base
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .filter(F.col("d.amount") >= 490)
    .select(
        "e.id",
        "e.order_id",
        "e.amount",
        "e.paid_at",
        "e.status",
        F.col("d.status").alias("order_status"),
    )
)

cleaned = (
    enriched
    .filter(F.col("order_id").isNotNull() & F.col("order_status").isin(["PENDING", "RETURNED", "SHIPPED"]))
    .select("*", F.col("paid_at").cast("date").alias("paid_at_day"))
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "amount", "status", "order_status", "paid_at_day")
)

with_windows = (
    ranked
    .select(
        "id",
        "paid_at_day",
        F.lag("amount").over(w2).alias("prev_amount"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

result = (
    with_windows
    .select("id", "paid_at_day", "prev_amount", "seq_no")
    .orderBy(F.col("seq_no").desc())
)
