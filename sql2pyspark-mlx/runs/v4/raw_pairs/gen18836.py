from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id")
w2 = Window.partitionBy("order_id", "method").orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("order_id", "status").orderBy("paid_at", "id")
w4 = Window.partitionBy("status").orderBy("paid_at", "id")
w5 = Window.partitionBy("method")
w6 = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("method").orderBy(F.col("row_seq").desc(), "id")

filtered = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-12-01").cast("date"))
    .select("*", F.round("amount", 1).alias("amount_rounded"))
)

enriched = (
    filtered
    .withColumn("status_avg_amount_rounded", F.avg("amount_rounded").over(w))
    .withColumn("amount_rounded_avg_2", F.avg("amount_rounded").over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
)

scored = (
    enriched
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w5)), 4).alias("amount_share"),
        F.sum("status_avg_amount_rounded").over(w6).alias("running_status_avg_amount_rounded"),
        F.row_number().over(w7).alias("row_seq_2"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("status_avg_amount_rounded") >= 75, "PREMIUM").otherwise("STANDARD").alias("status_avg_amount_rounded_tier"),
    )
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.min("paid_at").alias("first_paid_at"),
        F.percentile_approx("amount_share", 0.95).alias("amount_share_p95"),
        F.min("amount_rounded_avg_2").alias("min_amount_rounded_avg_2"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
    )
    .orderBy(F.col("status_avg_amount_rounded_tier").desc())
)
