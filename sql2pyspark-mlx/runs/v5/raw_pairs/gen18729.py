from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("status") == "OPEN") & F.col("opened_date").between("2024-05-13", "2025-09-01"))
    .select("id", "account_type")
)
