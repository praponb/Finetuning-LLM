from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 38)
    .select("customer_id", "status")
)
