from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") >= "2024-07-01")
    .select("id", "order_id")
    .unionByName(
        df_payments
        .filter(F.col("status").isin(["FAILED", "REFUNDED", "SUCCESS"]))
        .select("id", "order_id")
    )
    .distinct()
    .orderBy("id")
)
