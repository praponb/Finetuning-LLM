from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & F.col("status").isin(["FROZEN", "OPEN"]))
    .select("opened_date")
)
