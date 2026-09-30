from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("avg_balance_2").desc())

kept = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.amount") >= 1700), "left_anti")
)

result = (
    kept
    .groupBy(
        "status",
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
        F.date_trunc("MONTH", "opened_date").alias("activity_month"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "CLOSED", F.col("balance")).otherwise(0)).alias("closed_balance"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.round(F.avg("balance"), 2).alias("avg_balance_2"),
    )
    .withColumn("avg_balance_2_rank", F.rank().over(w))
    .orderBy("avg_balance")
)
