from pyspark.sql import functions as F

recent_payments = df_payments.filter((F.col("status") == "REFUNDED") & (F.col("status") != "SUCCESS"))

summary = (
    recent_payments
    .groupBy("method")
    .agg(
        F.countDistinct("amount").alias("unique_statuss"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    summary
    .filter(F.col("amount_range") < 61)
    .select("method", "unique_statuss", "amount_range")
    .orderBy(F.col("amount_range").desc(), "method")
    .limit(5)
)
