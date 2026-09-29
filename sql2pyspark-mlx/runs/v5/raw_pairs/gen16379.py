from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "CREDIT", "CHECKING"]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        F.upper("status").alias("status_upper"),
        F.coalesce("status", F.lit("OTHER")).alias("status_clean"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("balance") >= 65000, "HIGH").otherwise("LOW").alias("balance_tier"),
        "customer_id",
    )
    .agg(
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.count_if(F.col("account_type") == "CHECKING").alias("checking_events"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
)
