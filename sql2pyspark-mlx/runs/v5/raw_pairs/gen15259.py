from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.countDistinct("amount").alias("unique_statuss"),
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_count"),
    )
    .orderBy(F.col("method").desc())
)
