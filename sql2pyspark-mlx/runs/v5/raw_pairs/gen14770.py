from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") != "CHECKING") & (F.year("opened_date") >= 2025))
    .select("id", "opened_date", "customer_id", F.lower("status").alias("status_lower"))
    .orderBy("status_lower")
)
