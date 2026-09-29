from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc())
w2 = Window.partitionBy("status").orderBy("paid_at", "id")
w3 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_payments
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.method",
        "src.amount",
        "src.paid_at",
        F.col("p.order_date"),
        F.col("p.discount"),
        F.col("p.status").alias("order_status"),
    )
)

with_dims = (
    enriched
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .filter(F.col("p.status") == "RETURNED")
    .select(
        "s.id",
        "s.order_id",
        "s.amount",
        "s.paid_at",
        F.col("p.channel"),
        F.col("p.shipping_fee"),
        F.col("p.status"),
    )
)

deduped = (
    with_dims
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "amount", "paid_at", "channel", "shipping_fee", "status")
)

gaps = (
    deduped
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w2).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w2) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w3))

result = (
    islands
    .select(
        "id",
        "amount",
        "paid_at",
        "channel",
        "shipping_fee",
        "status",
        "is_new_burst",
        F.when(F.col("status").rlike("^(COMPLETED|PENDING)$"), "GROUP_B").when(F.col("status").rlike("^(CANCELLED|SHIPPED)$"), "PRIORITY").otherwise("GROUP_A").alias("status_zone"),
    )
    .orderBy(F.col("channel").desc_nulls_first())
)
