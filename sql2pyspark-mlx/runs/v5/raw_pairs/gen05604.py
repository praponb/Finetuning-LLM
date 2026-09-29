from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .withColumn("activity_month", F.date_trunc("MONTH", "paid_at"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "paid_at"))
    .cube("activity_month", "quarter_start")
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount"))).alias("success_total_amount"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_count"),
        F.grouping("activity_month").alias("grouping_level"),
    )
    .filter(F.col("success_total_amount") < 1000000)
)
