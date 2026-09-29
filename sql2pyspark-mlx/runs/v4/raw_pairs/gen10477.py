from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") != "CLOSED") & F.col("status").isin(["CLOSED", "FROZEN", "OPEN"]) & (F.year("opened_date") >= 2023))
    .select("customer_id", "status")
)
