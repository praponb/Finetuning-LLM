from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("NONE")))
)

result = (
    cleaned
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "inner")
    .cube(
        F.col("p.account_type").alias("account_type"),
        F.col("a.merchant").alias("merchant"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("a.amount")).alias("total_amount"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("rows_total").asc_nulls_last())
)
