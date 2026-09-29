from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").asc_nulls_last(), "id")

deduped = (
    df_accounts
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    deduped
    .rollup(
        F.when(F.col("balance") >= 62000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
        F.date_trunc("MONTH", "opened_date").alias("activity_month"),
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
    )
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.sum(F.when(F.col("status") == "CLOSED", F.col("balance")).otherwise(0)).alias("closed_balance"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
    )
    .orderBy("cnt")
)
