from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc_nulls_first())
w2 = Window.partitionBy("method").orderBy("paid_at", "id")
w3 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id", "status").orderBy(F.col("paid_at").desc(), "id")
w5 = Window.partitionBy("status").orderBy("amount_int", "id")
w6 = Window.orderBy("amount_int", "id")
w7 = Window.partitionBy("seq_no").orderBy(F.col("seq_no_per_refunded").desc())

deduped = (
    df_payments
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

cleaned = (
    deduped
    .filter(F.col("order_id").isNull())
    .withColumn("amount_int", F.col("amount").cast("int"))
    .withColumn("method_clean", F.coalesce("method", F.lit("N/A")))
)

flagged = (
    cleaned
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w2) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("session_no", F.sum("session_start_flag").over(w3))

deduped_payments = (
    sessions_payments
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "status", "amount_int")
)

enriched = (
    deduped_payments
    .filter(F.col("amount_int") <= 32)
    .select(
        "id",
        "status",
        F.lag("amount_int").over(w5).alias("prev_amount_int"),
        F.row_number().over(w5).alias("seq_no"),
        F.lag("amount_int", 2).over(w6).alias("prev_amount_int_2"),
    )
)

result = (
    enriched
    .groupBy("seq_no", "status")
    .agg(
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_rows"),
        F.min("prev_amount_int_2").alias("min_prev_amount_int_2"),
        F.round(F.sum("seq_no") / F.nullif(F.count(F.when(F.col("status") == "REFUNDED", 1)), F.lit(0)), 2).alias("seq_no_per_refunded"),
        F.count("*").alias("cnt"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.round(F.avg("prev_amount_int_2"), 2).alias("avg_prev_amount_int_2"),
    )
    .withColumn("seq_no_per_refunded_rank", F.rank().over(w7))
)
