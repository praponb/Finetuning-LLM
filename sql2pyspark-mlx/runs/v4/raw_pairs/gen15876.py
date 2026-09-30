from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.to_date("paid_at") < "2025-02-01") & (F.col("order_id") == 30))
    .select(
        "id",
        "order_id",
        F.col("amount").cast("int").alias("amount_int"),
        F.concat_ws("-", "method", "status").alias("method_status_key"),
    )
    .orderBy(F.col("id").desc(), "method_status_key")
    .limit(20)
)
