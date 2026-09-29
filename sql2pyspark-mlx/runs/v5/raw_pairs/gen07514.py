from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2024-02-25", "2024-08-01") & F.col("balance").between(20000, 75000) & F.col("status").isin(["CLOSED", "FROZEN"]))
    .select("id", "customer_id", "status")
    .orderBy(F.col("customer_id").desc(), "status")
)
