from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 53000)
    .select("id", "customer_id", "opened_date")
    .unionByName(
        df_accounts
        .filter(F.col("balance") >= 17000)
        .select("id", "customer_id", "opened_date")
    )
    .distinct()
)
