from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([22, 36, 39]))
    .select(
        "id",
        "customer_id",
        "account_type",
        F.when(F.col("balance") >= 71000, "High").when(F.col("balance") >= 27000, "Medium").otherwise("Low").alias("balance_level"),
        F.trim("account_type").alias("clean_account_type"),
    )
    .orderBy(F.col("account_type").desc(), F.col("clean_account_type").desc(), "id")
    .limit(5)
)
