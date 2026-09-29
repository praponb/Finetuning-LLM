from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .withColumn("method_length", F.length("method"))
    .withColumn("method_group", F.when(F.col("method").isin(["paypal", "card"]), "Priority").otherwise("Other"))
)

with_gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .rollup(
        F.when(F.col("is_new_session") >= 1, "LARGE").otherwise("SMALL").alias("is_new_session_tier"),
        "method",
        F.date_trunc("YEAR", "paid_at").alias("year_start"),
    )
    .agg(
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
        F.count("*").alias("cnt"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
    )
)
