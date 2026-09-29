from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "CREDIT", "SAVINGS"]))
    .select("customer_id", "status")
    .unionByName(
        df_accounts
        .filter(F.col("account_type").isin(["LOAN", "CREDIT"]))
        .select("customer_id", "status")
    )
    .distinct()
    .orderBy(F.col("customer_id").desc())
)
