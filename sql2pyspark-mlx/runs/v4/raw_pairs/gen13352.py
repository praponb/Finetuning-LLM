from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") != "cash")
    .groupBy("status")
    .agg(
        F.countDistinct("amount").alias("unique_orders"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("total_amount") < 6500)
    .orderBy(F.col("status").desc())
    .limit(10)
)
