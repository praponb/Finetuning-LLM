from pyspark.sql import functions as F

filtered = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-09-05 00:00:00").cast("timestamp")) & (F.col("paid_at") >= "2023-05-01"))
    .withColumn("status_title", F.initcap(F.trim("status")))
    .withColumn("method_clean", F.coalesce("method", F.lit("UNKNOWN")))
)

result = (
    filtered
    .select("id")
    .intersectAll(
        df_payments
        .filter(F.col("order_id").isin([10, 23, 35]))
        .select("id")
    )
)
