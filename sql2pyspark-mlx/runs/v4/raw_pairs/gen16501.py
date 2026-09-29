from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
    )
)
