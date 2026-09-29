from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "status", "account_type")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
    )
)
