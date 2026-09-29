from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
        F.date_trunc("MONTH", "paid_at").alias("paid_month"),
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.max("paid_at").alias("latest_paid_at"),
    )
    .filter(F.col("avg_amount") > 34)
)
