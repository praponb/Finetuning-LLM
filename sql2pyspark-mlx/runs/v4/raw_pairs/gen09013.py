from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select("id", "opened_date")
    .unionByName(
        df_accounts
        .filter(F.col("balance") >= 63000)
        .select("id", "opened_date")
    )
    .distinct()
)
