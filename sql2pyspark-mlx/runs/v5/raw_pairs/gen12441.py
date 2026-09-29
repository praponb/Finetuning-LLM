from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "FAILED") & (F.col("paid_at") >= "2025-09-12"))
    .select(
        "status",
        F.when(F.col("method").isin(["paypal", "card"]), "Group A").otherwise("Other").alias("method_group"),
    )
    .orderBy(F.col("method_group").desc())
)
