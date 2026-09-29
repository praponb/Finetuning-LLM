from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(1210, 1340))
    .select("id", "order_id")
    .unionByName(
        df_payments
        .filter(F.col("paid_at") >= "2024-10-01")
        .select("id", "order_id")
    )
    .distinct()
    .orderBy("id")
)
