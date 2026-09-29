from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", "order_id")
    .agg(
        F.countDistinct("amount").alias("unique_methods"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("amount_count") >= 3)
    .drop("amount_count")
)
