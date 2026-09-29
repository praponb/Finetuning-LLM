from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") >= "2025-02-01")
    .select(
        "id",
        "paid_at",
        "order_id",
        "method",
        F.initcap(F.trim("status")).alias("status_title"),
    )
    .orderBy("status_title")
)
