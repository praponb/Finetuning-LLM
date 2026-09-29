from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2025-06-18")
    .select("status", "method")
    .subtract(
        df_payments
        .filter(F.col("status") == "FAILED")
        .select("status", "method")
    )
    .orderBy("status")
)
