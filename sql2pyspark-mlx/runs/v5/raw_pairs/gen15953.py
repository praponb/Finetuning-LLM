from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "FROZEN"]))
    .select("customer_id")
    .unionByName(
        df_accounts
        .filter(F.col("account_type") == "CREDIT")
        .select("customer_id")
    )
    .distinct()
    .orderBy("customer_id")
)
