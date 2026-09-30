from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") >= 970) & F.col("method").isin(["promptpay", "card"]))
    .select(
        "id",
        "method",
        F.when(F.col("amount") >= 1260, "High").when(F.col("amount") >= 520, "Medium").otherwise("Low").alias("amount_tier"),
        F.trim("method").alias("clean_method"),
    )
    .orderBy(F.col("id").desc())
)
