from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("amount").between(670, 1030))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.concat_ws("-", "status", "method").alias("status_method_key"),
        F.length("status").alias("status_length"),
    )
)

with_gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_payments
    .groupBy(
        "burst_id",
        F.when(F.col("status_length") >= 64, "HIGH").otherwise("LOW").alias("status_length_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_rows"),
    )
    .filter(F.col("promptpay_rows") >= 1)
    .orderBy(F.col("promptpay_rows").desc_nulls_last())
)
