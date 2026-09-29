from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("card_events").desc())

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-05-13 09:00:00").cast("timestamp")) & F.col("order_id").isNotNull())
    .select(
        "*",
        F.concat("method", F.lit(", "), "status").alias("method_status"),
        F.to_date("paid_at").alias("paid_at_date"),
    )
)

with_gaps = (
    base
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .groupBy("status", F.date_trunc("MONTH", "paid_at_date").alias("activity_month"))
    .agg(
        F.count_if(F.col("method") == "card").alias("card_events"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_rows"),
    )
    .withColumn("card_events_dense_rank", F.dense_rank().over(w3))
)
