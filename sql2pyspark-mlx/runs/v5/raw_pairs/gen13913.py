from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-11-14").cast("date")) & F.col("order_id").isNull())
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        F.substring("method", 1, 3).alias("method_prefix"),
    )
)

result = (
    cleaned
    .select("status", "id")
    .intersectAll(
        df_payments
        .filter(F.col("status").isin(["REFUNDED", "FAILED"]))
        .select("status", "id")
    )
)
