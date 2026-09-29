from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "OPEN", "CLOSED"]) & (F.col("account_type") == "CREDIT"))
    .groupBy(
        "status",
        F.when(F.col("status").isin(["FROZEN", "CLOSED"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
    .filter(F.col("stddev_balance") > 40)
)
