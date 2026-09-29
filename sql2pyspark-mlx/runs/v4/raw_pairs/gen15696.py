from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.min("amount").alias("min_amount"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("min_amount") > 900)
    .select("status", "min_amount", "cnt")
)
