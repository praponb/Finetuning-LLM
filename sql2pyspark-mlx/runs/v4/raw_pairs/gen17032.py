from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "CREDIT")
    .agg(
        F.count("balance").alias("balance_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
    .orderBy("stddev_balance", F.col("balance_count").desc())
)
