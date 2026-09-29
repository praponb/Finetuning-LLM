from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .filter(F.col("t.merchant") == "Starbucks")
    .groupBy(
        F.when(F.year(F.col("a.opened_date")) < 2024, "before_2024").when(F.year(F.col("a.opened_date")) == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(F.min(F.col("t.amount")).alias("min_amount"))
    .orderBy("opened_cohort")
)
