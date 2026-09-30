from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.count("*").alias("cnt"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("cnt") < 3)
    .orderBy("account_id")
)
