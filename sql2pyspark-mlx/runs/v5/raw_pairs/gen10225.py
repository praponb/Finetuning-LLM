from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("avg_amount_int").desc())

cleaned = (
    df_payments
    .filter(F.col("method").isNotNull() & (F.col("paid_at") > F.lit("2023-08-30 17:00:00").cast("timestamp")))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.col("amount").cast("int").alias("amount_int"),
    )
)

result = (
    cleaned
    .groupBy("method", F.date_trunc("quarter", "paid_at").alias("quarter_start"))
    .agg(
        F.round(F.avg("amount_int"), 2).alias("avg_amount_int"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_rows"),
        F.count_if(F.col("status") == "FAILED").alias("failed_events"),
    )
    .withColumn("avg_amount_int_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("method").desc())
)
