from pyspark.sql import functions as F

valid = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .select(
        "*",
        F.round("amount", 2).alias("amount_rounded"),
        F.when(F.col("method").isin(["card", "promptpay"]), "Group A").otherwise("Other").alias("method_group"),
        F.coalesce("method", F.lit("N/A")).alias("method_clean"),
    )
)

result = (
    valid
    .groupBy("status")
    .pivot("method", ["promptpay", "cash"])
    .agg(F.avg("amount_rounded"))
)
