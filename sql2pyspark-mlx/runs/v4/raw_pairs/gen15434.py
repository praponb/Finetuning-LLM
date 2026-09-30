from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.year("opened_date") == 2023) | (F.col("balance") <= 27000))
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0)).alias("frozen_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(((100 * F.sum(F.when(F.col("account_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
    )
    .orderBy(F.col("opened_cohort").desc())
)
