from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").desc_nulls_last(), "id")

cleaned = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select("*", F.concat("status", F.lit(" - "), "method").alias("status_method"))
)

deduped = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

cleaned_payments = (
    deduped
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        "status_method",
        "dedup_rank",
        F.when(F.col("amount") >= 1220, "High").when(F.col("amount") >= 700, "Medium").otherwise("Low").alias("amount_band"),
        F.coalesce("method", F.lit("N/A")).alias("resolved_method"),
    )
)

filtered = (
    cleaned_payments
    .filter((F.col("paid_at") >= F.lit("2024-04-17").cast("date")) & F.col("order_id").isNotNull() & F.col("method").isin(["card", "paypal"]))
    .withColumn("is_success", F.when(F.col("status") == "SUCCESS", 1).otherwise(0))
)

grouped = (
    filtered
    .rollup(F.date_trunc("MONTH", "paid_at").alias("activity_month"), "status")
    .agg(
        F.count_if(F.col("method") == "card").alias("card_events"),
        F.min("paid_at").alias("first_paid_at"),
        F.round(F.avg("is_success"), 2).alias("avg_is_success"),
    )
)

result = (
    grouped
    .select("activity_month", "card_events", "first_paid_at", "avg_is_success")
    .orderBy(F.col("avg_is_success").desc(), "first_paid_at")
)
