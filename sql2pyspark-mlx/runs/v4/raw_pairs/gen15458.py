from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc_nulls_last())

deduped = (
    df_accounts
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy(F.date_trunc("QUARTER", "opened_date").alias("quarter_start"))
    .agg(
        F.sum(F.when(F.col("status") == "OPEN", F.col("balance")).otherwise(0)).alias("open_balance"),
        F.stddev_samp("balance").alias("balance_stddev"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
    )
    .orderBy(F.col("open_balance").asc_nulls_last())
)
