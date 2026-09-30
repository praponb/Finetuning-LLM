from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter((F.col("balance").between(44000, 57000) & (F.col("account_type") != "SAVINGS")) | ~F.col("status").isin(["CLOSED", "OPEN"]))
    .withColumn("status_lower", F.lower("status"))
)

summary = (
    filtered_accounts
    .groupBy(
        F.when(F.col("balance") >= 79000, "high").when(F.col("balance") >= 17000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.min("opened_date").alias("first_opened_date"))
)

result = summary
