from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-05-07", "2024-07-01"))
    .select("account_type")
    .intersect(
        df_accounts
        .filter(F.col("account_type") == "CREDIT")
        .select("account_type")
    )
)
