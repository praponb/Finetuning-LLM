from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (~F.col("status").eqNullSafe(F.lit("FROZEN"))))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
        F.when(F.col("account_type").isin(["CREDIT", "LOAN"]), "Group A").otherwise("Other").alias("account_type_group"),
        F.coalesce("status", F.lit("OTHER")).alias("status_clean"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("balance_filled") >= 63, "HIGH").otherwise("LOW").alias("balance_filled_tier"),
    )
    .agg(
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_count"),
        F.count("*").alias("cnt"),
        F.max(F.when(F.col("status") == "OPEN", F.col("balance"))).alias("open_max_balance"),
        F.avg("balance_filled").alias("avg_balance_filled"),
    )
    .orderBy(F.col("avg_balance_filled").desc_nulls_last())
)
