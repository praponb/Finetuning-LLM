from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("account_type") == "LOAN"))
)

summary = (
    base
    .groupBy("status")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = summary.filter(F.col("stddev_balance") >= 35)
