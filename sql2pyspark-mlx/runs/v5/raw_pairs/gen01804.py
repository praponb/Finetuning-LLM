from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select(
        "*",
        F.when(F.col("method").isin(["paypal", "bank_transfer"]), "Core").otherwise("Other").alias("method_group"),
        F.substring("method", 1, 3).alias("method_prefix"),
        F.coalesce("method", F.lit("OTHER")).alias("method_filled"),
    )
)

result = (
    base
    .groupBy("status", "order_id")
    .pivot("method", ["card", "promptpay"])
    .agg(F.max("amount").alias("amount"))
)
