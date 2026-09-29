from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("balance").isNotNull())
    .select(
        "*",
        F.length("account_type").alias("account_type_length"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_clean"),
    )
)

result = (
    base
    .alias("a")
    .join(df_customers.alias("dim"), F.col("a.customer_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["segment", "status"], ["segment"], []],
        F.col("dim.segment").alias("segment"),
        F.col("a.status").alias("status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("a.account_type_length")).alias("min_account_type_length"),
        F.grouping_id().alias("level_id"),
    )
)
