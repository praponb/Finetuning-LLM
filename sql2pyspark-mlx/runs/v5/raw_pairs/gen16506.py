from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_payments
    .groupBy(
        F.when(F.col("island_id") >= 9, "LARGE").otherwise("SMALL").alias("island_id_tier"),
        F.date_trunc("year", "paid_at").alias("year_start"),
        "island_id",
    )
    .agg(
        F.count_if(F.col("method") == "paypal").alias("paypal_events"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.count_if(F.col("method") == "paypal").alias("paypal_events_2"),
        F.count_if(F.col("method") == "paypal").alias("paypal_events_3"),
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("island_id")).otherwise(0)).alias("refunded_island_id"),
    )
    .orderBy("distinct_order_ids")
)
