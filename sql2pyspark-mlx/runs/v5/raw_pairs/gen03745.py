from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_payments
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_payments = (
    flagged
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_payments
    .groupBy("method", F.date_trunc("QUARTER", "paid_at").alias("quarter_start"))
    .agg(
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("burst_id")).otherwise(0)).alias("bank_transfer_burst_id"),
        F.max(F.when(F.col("method") == "promptpay", F.col("burst_id"))).alias("promptpay_max_burst_id"),
        F.round(F.sum("burst_id") / F.nullif(F.count(F.when(F.col("method") == "paypal", 1)), F.lit(0)), 2).alias("burst_id_per_paypal"),
        F.count_if(F.col("method") == "paypal").alias("paypal_events"),
        F.max(F.when(F.col("method") == "card", F.col("burst_id"))).alias("card_max_burst_id"),
        F.percentile_approx("burst_id", 0.9).alias("burst_id_p90"),
    )
)
