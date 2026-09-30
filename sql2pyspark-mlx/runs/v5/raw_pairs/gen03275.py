from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["FROZEN", "OPEN"]))
    .select(
        "status",
        "customer_id",
        "account_type",
        F.initcap(F.trim("status")).alias("status_title"),
    )
    .orderBy("status_title")
)
