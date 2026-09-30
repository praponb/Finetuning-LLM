from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("order_id") == 30)
    .select(
        "*",
        F.date_format("paid_at", "MMM yyyy").alias("paid_at_label"),
        F.col("paid_at").cast("date").alias("paid_at_day"),
    )
)

result = (
    base
    .select("order_id")
    .intersect(
        df_payments
        .filter(F.col("status") == "SUCCESS")
        .select("order_id")
    )
)
