from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 89000, "HIGH").otherwise("LOW").alias("balance_tier"),
    )
    .agg(
        F.countDistinct("account_type").alias("distinct_account_types"),
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance")).otherwise(0)).alias("checking_balance"),
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_rows"),
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("account_type") == "SAVINGS", 1)), F.lit(0)), 2).alias("balance_per_savings"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.percentile_approx("balance", 0.95).alias("balance_p95"),
    )
    .filter(F.col("distinct_account_types") < 1000)
)
