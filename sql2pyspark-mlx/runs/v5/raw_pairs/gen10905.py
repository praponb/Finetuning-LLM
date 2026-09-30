from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") < "2025-09-01")
    .groupBy("account_type", F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("balance")).otherwise(0)).alias("credit_balance"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
    )
    .filter(F.col("credit_balance") > 62)
    .orderBy("credit_balance")
)
