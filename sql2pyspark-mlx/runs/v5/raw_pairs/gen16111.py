from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") > 52000)
    .select("opened_date")
    .subtract(df_inventory.select("last_restock"))
)
