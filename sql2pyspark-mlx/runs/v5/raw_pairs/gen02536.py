from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.countDistinct("balance").alias("unique_statuss"),
    )
    .orderBy("balance_range")
)
