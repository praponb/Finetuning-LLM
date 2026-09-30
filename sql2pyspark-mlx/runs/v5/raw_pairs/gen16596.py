from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("amount") >= 1580, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.count_if(F.col("method") == "paypal").alias("paypal_events"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.max("paid_at").alias("ended_at"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.min("paid_at").alias("first_paid_at"),
    )
    .orderBy(F.col("num_status").asc_nulls_last())
)
