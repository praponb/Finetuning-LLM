from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "promptpay")
    .select("method")
    .unionByName(df_payments.filter(F.col("order_id") == 25).select("method"))
    .distinct()
)
