from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") != "SAVINGS")
    .groupBy(
        F.when(F.col("balance") >= 74000, "high").when(F.col("balance") >= 30000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
    )
    .filter(F.col("balance_range") < 52)
    .orderBy(F.col("balance_range").desc(), "balance_bucket")
)
