from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") != "FROZEN")
    .select("status")
    .subtract(
        df_payments
        .filter(F.to_date("paid_at") >= "2025-10-28")
        .select("status")
    )
)
