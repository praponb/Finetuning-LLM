from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-06-09").cast("date")) & F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        F.floor("balance").alias("balance_whole"),
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

result = (
    filtered
    .select("account_type")
    .subtract(
        df_accounts
        .filter(F.col("balance") > 40000)
        .select("account_type")
    )
)
