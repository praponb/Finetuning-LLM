from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("status") != "CLOSED") & (F.col("balance") < 55000))
    .select("id", "balance", "status", "opened_date")
)
