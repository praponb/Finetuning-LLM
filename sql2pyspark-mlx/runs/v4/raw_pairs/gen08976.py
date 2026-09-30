from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") < 0.16)
    .select("status")
    .unionByName(
        df_payments
        .filter(F.col("method").isin(["cash", "promptpay", "bank_transfer"]))
        .select("status")
    )
)
