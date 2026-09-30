from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.year("opened_date") == 2024)
    .select("balance", "customer_id")
    .orderBy(F.col("balance").desc(), F.col("customer_id").desc())
)
