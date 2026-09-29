from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy(F.col("opened_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("balance_tier").orderBy(F.col("balance_p95").desc())

latest = (
    df_accounts
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest
    .groupBy(
        F.when(F.col("balance") >= 75000, "HIGH").otherwise("LOW").alias("balance_tier"),
        F.date_trunc("YEAR", "opened_date").alias("year_start"),
    )
    .agg(
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.percentile_approx("balance", 0.95).alias("balance_p95"),
    )
    .filter(F.col("closed_count") > 1)
    .withColumn("balance_p95_percent_rank", F.percent_rank().over(w2))
    .orderBy(F.col("balance_p95").desc_nulls_first())
)
