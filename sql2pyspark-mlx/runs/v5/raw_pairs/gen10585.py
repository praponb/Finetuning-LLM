from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-05-25").cast("date")) & (~F.col("method").eqNullSafe(F.lit("promptpay"))))
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        F.col("amount").cast("int").alias("amount_int"),
    )
)

result = (
    base
    .withColumn("activity_month", F.date_trunc("MONTH", "paid_at"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "paid_at"))
    .cube("method", "activity_month", "quarter_start")
    .agg(
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_rows"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.grouping("method").alias("gid"),
    )
    .filter(F.col("distinct_ids") < 1000)
)
