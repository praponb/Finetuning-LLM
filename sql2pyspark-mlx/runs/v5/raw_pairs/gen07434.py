from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") > 1000)
    .select("id", "method", "order_id")
    .unionByName(
        df_payments
        .filter(F.col("method").isNotNull())
        .select("id", "method", "order_id")
    )
    .distinct()
)
