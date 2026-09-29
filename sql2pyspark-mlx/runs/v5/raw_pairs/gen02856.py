from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["cash", "paypal"]) & (F.col("method") == "bank_transfer"))
    .groupBy(F.quarter("paid_at").alias("paid_quarter"))
    .agg(
        F.count("*").alias("cnt"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy("cnt", "paid_quarter")
)
