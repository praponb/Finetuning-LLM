from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter((~F.col("status").eqNullSafe(F.lit("FROZEN"))) & F.col("status").isNotNull())
    .select("*", F.length("account_type").alias("account_type_length"))
)

result = (
    filtered
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .cube(F.col("d.city").alias("city"), F.col("src.status").alias("status"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("src.balance")).alias("total_balance"),
        F.max(F.col("src.account_type_length")).alias("max_account_type_length"),
        F.grouping_id().alias("grouping_level"),
    )
)
