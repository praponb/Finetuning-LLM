from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.to_date("paid_at") < "2024-01-28")
    .select("paid_at", "status")
)
