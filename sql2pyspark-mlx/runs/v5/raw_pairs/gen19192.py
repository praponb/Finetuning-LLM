from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "paypal")
    .select("status", "method")
    .unionByName(
        df_payments
        .filter(F.col("order_id").isin([1, 30]))
        .select("status", "method")
    )
)
