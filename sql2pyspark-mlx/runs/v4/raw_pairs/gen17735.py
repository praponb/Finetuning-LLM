from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .rollup(
        F.when(F.col("is_new_burst") >= 1, "HIGH").otherwise("LOW").alias("is_new_burst_tier"),
    )
    .agg(
        F.round(F.sum("is_new_burst") / F.nullif(F.count(F.when(F.col("status") == "FAILED", 1)), F.lit(0)), 2).alias("is_new_burst_per_failed"),
        F.sum(F.when(F.col("method") == "promptpay", F.col("amount")).otherwise(0)).alias("promptpay_amount"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.min("paid_at").alias("started_at"),
        F.count_if(F.col("status") == "REFUNDED").alias("refunded_events"),
        F.max("paid_at").alias("ended_at"),
    )
    .filter(F.col("refunded_events") < 1)
    .orderBy(F.col("refunded_events").desc())
)
