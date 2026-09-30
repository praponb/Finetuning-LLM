from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").isNotNull())
    .select("status", "order_id")
    .unionByName(
        df_payments
        .filter(F.col("amount").between(670, 920))
        .select("status", "order_id")
    )
    .orderBy("status")
)
