from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") != "LOAN")
    .groupBy(
        F.when(F.col("account_type").isin(["CHECKING", "SAVINGS"]), "tier_1").otherwise("other").alias("account_type_group"),
    )
    .agg(
        F.count("balance").alias("balance_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
    .orderBy(F.col("balance_count").desc(), F.col("account_type_group").desc())
    .limit(3)
)
