from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([10, 17, 23, 31]))
    .select("id", "opened_date", F.lower("account_type").alias("account_type_lower"))
    .orderBy("opened_date")
)
