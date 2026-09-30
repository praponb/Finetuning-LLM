from pyspark.sql import functions as F

result = (
    df_transactions
    .agg(
        F.min("amount").alias("min_amount"),
        F.countDistinct("amount").alias("unique_merchants"),
    )
)
