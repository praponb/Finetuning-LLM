from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") < 1560)
    .select("id", "status", "method")
    .unionByName(
        df_payments
        .filter(F.col("paid_at") >= "2023-09-20")
        .select("id", "status", "method")
    )
    .distinct()
)
