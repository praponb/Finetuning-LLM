from pyspark.sql import functions as F

filtered_accounts = df_accounts.filter(~((F.col("customer_id") == 7) & (F.col("balance") <= 61000)))

agg = (
    filtered_accounts
    .groupBy(
        F.when(F.col("balance") >= 70000, "high").when(F.col("balance") >= 21000, "mid").otherwise("low").alias("balance_bucket"),
        "account_type",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
        F.round(((100 * F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_closed"),
    )
)

result = (
    agg
    .select("balance_bucket", "account_type_values", "pct_closed")
    .orderBy("balance_bucket", "pct_closed")
)
