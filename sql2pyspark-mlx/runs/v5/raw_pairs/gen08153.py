from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.to_date("paid_at") < "2025-02-17") | F.hour("paid_at").between(9, 15))
)

agg = (
    base
    .groupBy("method")
    .agg(
        F.min("paid_at").alias("min_paid_at"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    agg
    .select("method", "min_paid_at", "amount_count")
    .orderBy("min_paid_at", "method")
    .limit(50)
)
