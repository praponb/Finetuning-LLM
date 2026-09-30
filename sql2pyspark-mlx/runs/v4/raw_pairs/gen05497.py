from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") > 24000)
    .select("id", "opened_date")
    .unionByName(
        df_accounts
        .filter(F.col("status") == "CLOSED")
        .select("id", "opened_date")
    )
    .distinct()
)
