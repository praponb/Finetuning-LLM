from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("store_id").orderBy("paid_at", "id")
w3 = Window.partitionBy("store_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())

latest_match = (
    df_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.discount",
        F.col("c.paid_at"),
        F.col("c.amount").alias("payment_amount"),
    )
)

with_prev = latest_match.withColumn("prev_paid_at", F.lag("paid_at").over(w2))

gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "store_id",
        "paid_at",
        F.sum("is_new_session").over(w3).alias("burst_id"),
    )
)

with_last = (
    islands
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price"))
)

with_last_orders = (
    with_last
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.method").alias("payment_method"),
        F.col("c.status").alias("payment_status"),
    )
)

wide = (
    with_last_orders
    .groupBy("payment_method")
    .pivot("payment_status", ["SUCCESS", "FAILED"])
    .agg(F.sum("unit_price"))
)

result = (
    wide
    .select("payment_method", "SUCCESS", "FAILED")
    .orderBy("FAILED", F.col("SUCCESS").desc())
)
