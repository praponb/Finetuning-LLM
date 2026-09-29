from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("opened_date") < "2023-07-01")
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.concat_ws("/", "status", "account_type").alias("status_account_type_key"),
    )
)

result = (
    base
    .groupBy("status")
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy(F.col("status").desc())
)
