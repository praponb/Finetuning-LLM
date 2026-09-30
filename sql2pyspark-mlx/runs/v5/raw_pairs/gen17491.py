from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~(F.col("method").isin(["promptpay", "bank_transfer", "card"]) & (F.col("method") == "bank_transfer")))
    .select(
        "id",
        "order_id",
        "status",
        "amount",
        F.when(F.col("method") == "promptpay", 1).otherwise(0).alias("is_promptpay"),
        F.when(F.col("method") == "card", 1).otherwise(0).alias("is_card"),
    )
    .orderBy(F.col("is_promptpay").desc())
)
