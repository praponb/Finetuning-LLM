from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter(F.col("method").isNotNull())
    .withColumn("amount_int", F.col("amount").cast("int"))
    .withColumn("resolved_method", F.coalesce("method", F.lit("OTHER")))
)

flagged = (
    cleaned
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessions_payments
    .groupBy("method")
    .agg(
        F.avg(F.when(F.col("method") == "promptpay", F.col("amount_int"))).alias("promptpay_avg_amount_int"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("method") == "card", 1)).alias("card_rows"),
        F.sum(F.when(F.col("method") == "paypal", F.col("session_start_flag")).otherwise(0)).alias("paypal_session_start_flag"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_rows"),
    )
    .orderBy(F.col("method").desc())
)
