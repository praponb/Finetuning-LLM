from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["bank_transfer", "paypal"]))
    .groupBy(F.year("paid_at").alias("paid_year"))
    .agg(
        F.count("*").alias("total_count"),
        F.min("paid_at").alias("first_paid_at"),
    )
    .filter(F.col("total_count") < 8)
    .select("paid_year", "total_count", "first_paid_at")
)
