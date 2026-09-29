from pyspark.sql import functions as F

src = df_payments.filter(F.col("paid_at") < "2024-12-23")

paid_quarter_summary = (
    src
    .groupBy(F.quarter("paid_at").alias("paid_quarter"))
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    paid_quarter_summary
    .orderBy(F.col("avg_amount").desc(), "paid_quarter")
    .limit(3)
)
