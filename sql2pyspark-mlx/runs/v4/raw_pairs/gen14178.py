from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2024-01-08", "2024-11-01"))
    .select("opened_date", "customer_id", "balance")
)
