from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") >= "2023-09-01")
    .select("id", "order_id", "status")
    .unionByName(
        df_payments
        .filter(~F.col("status").isin(["REFUNDED", "SUCCESS"]))
        .select("id", "order_id", "status")
    )
    .distinct()
)
