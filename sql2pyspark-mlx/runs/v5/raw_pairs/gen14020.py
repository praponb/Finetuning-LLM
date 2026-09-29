from pyspark.sql import functions as F

filtered_accounts = df_accounts.filter(F.col("balance") < 45000)

accounts_stats = (
    filtered_accounts
    .groupBy("account_type")
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
    )
)

result = (
    accounts_stats
    .select("account_type", "balance_range", "account_type_list")
    .orderBy(F.col("balance_range").desc())
)
