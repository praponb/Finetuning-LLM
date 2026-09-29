from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "CLOSED") & ((F.col("customer_id") == 30) | F.col("status").isNull()))
    .select("id", "balance", F.upper("account_type").alias("account_type_upper"))
)
