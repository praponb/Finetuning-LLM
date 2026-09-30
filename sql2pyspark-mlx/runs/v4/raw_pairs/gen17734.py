from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2023-08-10")
    .select("method", "status")
    .unionByName(
        df_payments
        .filter(F.col("status").isin(["SUCCESS", "FAILED", "REFUNDED"]))
        .select("method", "status")
    )
    .distinct()
    .orderBy(F.col("method").desc())
)
