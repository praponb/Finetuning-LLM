from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2024-03-23").cast("date")) & F.col("status").isNotNull())
    .select(
        "id",
        "account_type",
        F.length("account_type").alias("account_type_length"),
        F.coalesce("status", F.lit("NONE")).alias("status_filled"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("account_type_length") >= 68, "LARGE").otherwise("SMALL").alias("account_type_length_tier"),
    )
    .agg(
        F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_count"),
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("account_type_length")).otherwise(0)).alias("credit_account_type_length"),
        F.round(F.avg("account_type_length"), 2).alias("avg_account_type_length"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max(F.when(F.col("account_type") == "CHECKING", F.col("account_type_length"))).alias("checking_max_account_type_length"),
        F.count_if(F.col("account_type") == "CHECKING").alias("checking_events"),
    )
    .filter(F.col("avg_account_type_length") < 0)
)
