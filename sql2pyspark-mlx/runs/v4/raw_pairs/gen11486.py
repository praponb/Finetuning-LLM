from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "CREDIT")
    .select("id", "opened_date")
    .unionByName(
        df_accounts
        .filter(F.col("customer_id") == 10)
        .select("id", "opened_date")
    )
    .orderBy("opened_date")
)
