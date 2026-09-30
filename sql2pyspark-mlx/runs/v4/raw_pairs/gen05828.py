from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id") == 29) | F.col("method").isin(["cash", "promptpay", "paypal"]))
)
