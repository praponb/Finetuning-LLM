from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") >= 1050) | (F.col("method") == "promptpay"))
    .select(
        "id",
        "order_id",
        F.when(F.col("method").isin(["promptpay", "card"]), "Core").otherwise("Other").alias("method_group"),
        F.round("amount", 0).alias("amount_rounded"),
    )
)
