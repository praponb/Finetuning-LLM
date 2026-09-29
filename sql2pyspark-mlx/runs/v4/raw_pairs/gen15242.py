from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "paid_at",
        "amount",
        F.initcap(F.trim("method")).alias("method_title"),
        F.substring("method", 1, 3).alias("method_prefix"),
    )
)
