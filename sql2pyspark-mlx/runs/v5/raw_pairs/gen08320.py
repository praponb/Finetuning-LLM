from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(((F.col("opened_date") <= "2023-03-01") & (F.col("balance") <= 39000)) | F.col("balance").isNull())
    .withColumn("adjusted_balance", F.round((F.col("balance") * 1.07), 2))
    .withColumn("status_filled", F.coalesce("status", F.lit("Unknown")))
    .orderBy("customer_id")
)
