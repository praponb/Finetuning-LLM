from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") <= 38000)
    .groupBy("account_type")
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.sum("balance").alias("total_balance"),
    )
)
