from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") != "SUCCESS") & F.col("amount").between(910, 1070) & (F.col("method") == "card"))
    .groupBy("method", "status")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.min("amount").alias("min_amount"),
    )
)
