from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter(F.col("balance") < 22000)
    .withColumn("is_savings", F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0))
    .withColumn("opened_date_minus_3d", F.date_sub("opened_date", 3))
    .withColumn("status_clean", F.coalesce("status", F.lit("NONE")))
)

result = (
    filtered
    .groupBy(F.date_trunc("week", "opened_date").alias("week_start"))
    .agg(
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"),
    )
    .orderBy("week_start")
)
