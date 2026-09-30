from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-11-01", "2024-04-01"))
    .select("id", "opened_date")
    .unionByName(
        df_accounts
        .filter(F.col("customer_id").isNull())
        .select("id", "opened_date")
    )
)
