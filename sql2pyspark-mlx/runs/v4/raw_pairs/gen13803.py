from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(480, 1370))
    .select("status", "order_id")
    .unionByName(
        df_payments
        .filter(F.col("amount") >= 1380)
        .select("status", "order_id")
    )
)
