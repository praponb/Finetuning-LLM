from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy("paid_at", "id")
w4 = Window.partitionBy("channel").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select("*", F.round((F.col("amount") * 1.07), 2).alias("amount_adjusted"))
)

with_gaps = (
    cleaned
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

with_dims = (
    sessions_payments
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .select(
        "s.*",
        F.col("p.order_date"),
        F.col("p.shipping_fee"),
        F.col("p.status").alias("order_status"),
    )
)

joined = (
    with_dims
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.channel"))
)

with_dims_payments = (
    joined
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.amount").alias("order_amount"))
)

gaps = (
    with_dims_payments
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w3) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id_2", F.sum("is_new_session").over(w4))

result = (
    sessionized
    .select(
        "id",
        "amount",
        "amount_adjusted",
        "new_group_flag",
        "island_id",
        "order_date",
        "shipping_fee",
        "order_status",
        "order_amount",
        F.when(F.col("order_status").rlike("^(PENDING|COMPLETED)$"), "EXTENDED").otherwise("CORE").alias("order_status_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("order_status").desc())
)
