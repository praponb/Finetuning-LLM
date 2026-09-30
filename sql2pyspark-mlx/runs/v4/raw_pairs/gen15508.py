from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type")
w2 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(-5, Window.currentRow)

scored = (
    df_accounts
    .select(
        "*",
        F.stddev_samp("balance").over(w).alias("account_type_stddev_samp_balance"),
        F.avg("balance").over(w2).alias("balance_avg_5"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("account_type_stddev_samp_balance") >= 81726, "LARGE").otherwise("SMALL").alias("account_type_stddev_samp_balance_tier"),
        "status",
    )
    .agg(
        F.percentile_approx("balance_avg_5", 0.9).alias("balance_avg_5_p90"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count_2"),
    )
    .orderBy(F.col("account_type_stddev_samp_balance_tier").desc_nulls_first())
)
