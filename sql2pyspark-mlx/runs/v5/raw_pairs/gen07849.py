from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") <= 71000)
    .select("id", "opened_date", "customer_id", "status")
    .orderBy("opened_date")
)
