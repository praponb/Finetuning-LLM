from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at")

latest = (
    df_payments
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("activity_month"), "status")
    .agg(
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("method") == "bank_transfer", 1)), F.lit(0)), 2).alias("amount_per_bank_transfer"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("method") == "promptpay", 1)), F.lit(0)), 2).alias("amount_per_promptpay"),
    )
    .filter(F.col("amount_per_bank_transfer") > 0)
    .orderBy(F.col("status").desc())
)
