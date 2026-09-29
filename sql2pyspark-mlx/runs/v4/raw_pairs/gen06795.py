from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2024-01-16").cast("date")) & F.col("method").isNotNull())
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.initcap(F.trim("method")).alias("method_title"),
    )
)

with_gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

gaps = (
    sessionized
    .withColumn("is_new_burst_2", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized_payments = gaps.withColumn("session_no", F.sum("is_new_burst_2").over(w2))

result = (
    sessionized_payments
    .groupBy(
        F.when(F.col("session_no") >= 6, "PREMIUM").otherwise("STANDARD").alias("session_no_tier"),
    )
    .agg(
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_rows"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_rows"),
        F.max(F.when(F.col("status") == "SUCCESS", F.col("is_new_burst_2"))).alias("success_max_is_new_burst_2"),
    )
    .filter(F.col("paypal_rows") >= 1)
    .orderBy(F.col("cash_rows").desc())
)
