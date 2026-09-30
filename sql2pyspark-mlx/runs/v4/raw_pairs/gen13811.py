from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("avg_balance") < 52000)
    .select("account_type", "avg_balance", "status_list")
)
