from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("payment_status").orderBy("paid_at", "id")
w3 = Window.partitionBy("payment_status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("is_new_session", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("payment_status", "status").orderBy("last3_count_island_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy("island_id", "id")

with_latest = (
    df_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.order_date",
        "s.status",
        F.col("c.status").alias("payment_status"),
        F.col("c.paid_at"),
    )
)

flagged = (
    with_latest
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w2).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w2) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_session").over(w3))

flagged_2 = (
    islands
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w2) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands_orders = flagged_2.withColumn("session_id", F.sum("session_start_flag").over(w3))

metrics = (
    islands_orders
    .withColumn("last3_sum_island_id", F.sum("island_id").over(w4))
    .withColumn("last3_count_island_id", F.count("island_id").over(w4))
    .withColumn("last3_avg_island_id", F.avg("island_id").over(w4))
)

kept = (
    metrics
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.product_id") == 40), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "store_id",
        "order_date",
        "paid_at",
        "last3_sum_island_id",
        "last3_count_island_id",
        F.count("is_new_session").over(w5).alias("running_count_is_new_session"),
        F.sum("is_new_session").over(w5).alias("running_sum_is_new_session"),
        F.min("is_new_session").over(w5).alias("running_min_is_new_session"),
        F.ntile(10).over(w6).alias("island_id_bucket"),
    )
)
