from pyspark.sql import functions as F

src = (
    df_payments
    .filter(~((F.col("order_id") == 25) & (F.col("order_id").isin([8, 28, 34, 37]) | (F.to_date("paid_at") < "2024-10-01"))))
)

status_summary = (
    src
    .groupBy("status", "method")
    .agg(
        F.min("paid_at").alias("min_paid_at"),
        F.countDistinct("amount").alias("unique_statuss"),
    )
)

result = (
    status_summary
    .filter(F.col("unique_statuss") < 3)
    .select("status", "min_paid_at", "unique_statuss")
    .orderBy(F.col("unique_statuss").desc())
)
