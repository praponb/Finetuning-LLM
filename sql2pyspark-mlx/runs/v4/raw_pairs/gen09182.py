from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull() & (F.col("method").isin(["bank_transfer", "paypal"]) | F.col("order_id").isNull()))
    .select(
        "id",
        "order_id",
        "method",
        "status",
        F.trim("method").alias("clean_method"),
        F.when(F.col("amount") >= 1710, "High").when(F.col("amount") >= 350, "Medium").otherwise("Low").alias("amount_band"),
    )
)
