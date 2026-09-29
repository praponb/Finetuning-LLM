from pyspark.sql import functions as F

result = (
    df_payments
    .select("id")
    .intersect(
        df_payments
        .filter(F.col("paid_at") >= "2024-09-01")
        .select("id")
    )
)
