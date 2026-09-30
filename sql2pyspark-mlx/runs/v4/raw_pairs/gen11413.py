from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["promptpay", "card", "paypal"]))
    .select(
        "id",
        "method",
        "status",
        "paid_at",
        F.concat_ws("-", "status", "method").alias("status_method_key"),
    )
)
