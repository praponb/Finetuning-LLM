from pyspark.sql import functions as F

filtered_accounts = df_accounts.filter(F.col("customer_id").isin([14, 26, 35]))

status_group_summary = (
    filtered_accounts
    .groupBy(
        F.when(F.col("status").isin(["FROZEN", "OPEN"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
    )
)

result = (
    status_group_summary
    .select("status_group", "pct_credit")
    .orderBy("pct_credit")
)
