from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["cash", "paypal", "promptpay"]))
    .select(
        "order_id",
        "paid_at",
        F.concat("status", F.lit(", "), "method").alias("status_method"),
    )
)
