from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "FAILED")
    .select(
        "order_id",
        F.when(F.col("method").isin(["paypal", "promptpay"]), "Group A").otherwise("Other").alias("method_group"),
    )
)
