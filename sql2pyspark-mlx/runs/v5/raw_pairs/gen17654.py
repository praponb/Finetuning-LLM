from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-06-01", "2023-10-01"))
    .select("id", "account_type")
    .unionByName(
        df_accounts
        .filter(F.col("balance") < 65000)
        .select("id", "account_type")
    )
)
