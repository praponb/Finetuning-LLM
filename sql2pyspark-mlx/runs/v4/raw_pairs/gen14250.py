from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("account_type") == "SAVINGS", F.col("balance")).otherwise(0)).alias("savings_balance"),
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
    )
    .filter(F.col("savings_balance") >= 73)
)
