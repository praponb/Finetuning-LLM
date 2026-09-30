from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") > 42000)
    .select("id", "opened_date")
)
