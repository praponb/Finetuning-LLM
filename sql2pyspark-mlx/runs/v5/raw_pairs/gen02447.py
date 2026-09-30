from pyspark.sql import functions as F

recent_accounts = df_accounts.filter(F.col("status") != "CLOSED")

agg = (
    recent_accounts
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.avg("balance").alias("avg_balance"),
    )
)

result = (
    agg
    .filter(F.col("avg_balance") >= 42000)
    .select("opened_month", "status_list")
    .orderBy(F.col("opened_month").desc())
)
