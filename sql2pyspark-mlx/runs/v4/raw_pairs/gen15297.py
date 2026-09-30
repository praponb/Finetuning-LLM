from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .unionByName(
        df_payments
        .filter(F.col("method") == "bank_transfer")
        .select("status")
    )
)
