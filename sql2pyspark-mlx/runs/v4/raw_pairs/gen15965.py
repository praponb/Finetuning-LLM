from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("paid_at", "id")
w4 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("method") != "promptpay")
    .withColumn("method_lower", F.lower("method"))
    .withColumn("method_prefix", F.substring("method", 1, 4))
)

with_prev = valid.withColumn("prev_paid_at", F.lag("paid_at").over(w))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("new_group_flag").over(w2))

flagged_2 = (
    islands
    .withColumn("new_group_flag_2", F.when(F.col("paid_at") - F.lag("paid_at").over(w3) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands_payments = flagged_2.withColumn("burst_id", F.sum("new_group_flag_2").over(w4))

result = (
    islands_payments
    .groupBy("order_id")
    .pivot("status", ["FAILED", "REFUNDED"])
    .agg(F.sum("amount").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
