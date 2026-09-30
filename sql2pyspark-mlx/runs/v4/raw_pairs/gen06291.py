from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") > 56000) & F.col("status").isin(["CLOSED", "FROZEN", "OPEN"]))
    .select(
        "id",
        "status",
        "customer_id",
        "balance",
        F.initcap(F.trim("status")).alias("status_title"),
        F.concat("status", F.lit(" - "), "account_type").alias("status_account_type"),
    )
    .orderBy(F.col("id").desc())
)
