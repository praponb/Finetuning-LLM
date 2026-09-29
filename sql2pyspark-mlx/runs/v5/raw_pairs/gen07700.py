from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") >= 1460)
    .select("shipping_fee")
    .unionByName(
        df_payments
        .filter(F.col("status") == "FAILED")
        .select(F.col("amount").alias("shipping_fee"))
    )
)
