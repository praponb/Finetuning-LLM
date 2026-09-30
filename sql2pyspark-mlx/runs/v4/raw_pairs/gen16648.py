from pyspark.sql import functions as F

result = (
    df_payments
    .select("method", "order_id")
    .exceptAll(
        df_payments
        .filter(F.col("method").isin(["bank_transfer", "promptpay"]))
        .select("method", "order_id")
    )
)
