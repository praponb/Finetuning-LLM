from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "REFUNDED", "SUCCESS"]))
    .select("id", "method", "order_id")
    .unionByName(
        df_payments
        .filter(F.col("order_id").isin([11, 22, 38]))
        .select("id", "method", "order_id")
    )
    .distinct()
)
