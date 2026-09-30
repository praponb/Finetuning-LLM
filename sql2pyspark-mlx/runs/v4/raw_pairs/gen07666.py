from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 71000)
    .agg(
        F.sum(F.when(F.col("account_type") == "SAVINGS", F.col("balance")).otherwise(0)).alias("savings_balance"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
    )
    .orderBy(F.col("savings_balance").desc())
)
