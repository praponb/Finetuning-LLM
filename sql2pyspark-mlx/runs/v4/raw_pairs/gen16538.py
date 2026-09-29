from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("balance_tier").orderBy(F.col("num_status").desc())

kept_keys = (
    df_accounts
    .select("account_type")
    .subtract(
        df_accounts
        .filter(F.col("status") != "OPEN")
        .select("account_type")
    )
)

kept_rows = (
    df_accounts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("balance") >= 79000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
        F.date_trunc("YEAR", "opened_date").alias("year_start"),
    )
    .agg(
        F.max("balance").alias("max_balance"),
        F.count_if(F.col("account_type") == "SAVINGS").alias("savings_events"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("savings_events") < 1)
    .withColumn("num_status_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("max_balance").asc_nulls_last())
)
