from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter((F.col("account_type") != "CHECKING") & F.col("balance").between(28000, 58000) & F.col("customer_id").isin([25, 26]))
    .withColumn("opened_date_label", F.date_format("opened_date", "yyyy-MM"))
)

summary = (
    recent_accounts
    .groupBy(
        F.when(F.col("balance") >= 61000, "high").when(F.col("balance") >= 27000, "mid").otherwise("low").alias("balance_bucket"),
        F.quarter("opened_date").alias("opened_quarter"),
    )
    .agg(F.max("opened_date").alias("last_opened_date"))
)

result = (
    summary
    .select("balance_bucket", "opened_quarter", "last_opened_date")
    .orderBy(F.col("last_opened_date").desc())
)
