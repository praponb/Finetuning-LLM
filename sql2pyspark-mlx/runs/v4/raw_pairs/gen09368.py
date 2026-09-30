from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(810, 1440) & F.col("order_id").isin([4, 15, 28]))
    .select(
        "status",
        "order_id",
        "paid_at",
        F.initcap(F.trim("method")).alias("method_title"),
    )
)
