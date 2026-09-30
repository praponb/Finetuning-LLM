from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").isNull() & (F.col("balance") < 17000) & F.col("account_type").isin(["SAVINGS", "LOAN"]))
    .groupBy("status")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
    .orderBy("status")
    .limit(5)
)
