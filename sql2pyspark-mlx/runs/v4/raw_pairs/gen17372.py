from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(850, 1470))
    .select("order_id", "method")
    .unionByName(
        df_payments
        .filter(F.col("method").isin(["cash", "paypal"]))
        .select("order_id", "method")
    )
)
