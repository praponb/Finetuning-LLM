from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") > "2024-01-12") & (F.col("balance") > 41000) & (F.col("account_type") == "CHECKING"))
    .select("account_type", "balance")
    .orderBy("account_type")
)
