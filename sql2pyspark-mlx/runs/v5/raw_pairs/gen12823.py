from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~((F.col("status") == "CLOSED") & (F.year("opened_date") == 2024) & F.col("customer_id").isNotNull()))
    .select("id", "balance")
)
