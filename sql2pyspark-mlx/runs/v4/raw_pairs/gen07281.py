from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "FAILED"]))
    .select("order_id", "method")
    .subtract(
        df_payments
        .filter(F.to_date("paid_at") < "2025-02-13")
        .select("order_id", "method")
    )
    .orderBy("order_id")
)
