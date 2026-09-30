from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 84000, "LARGE").otherwise("SMALL").alias("balance_tier"),
    )
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.count_if(F.col("status") == "FROZEN").alias("frozen_events"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sort_array(F.collect_set("status")).alias("status_list_2"),
        F.sum("balance").alias("total_balance"),
    )
)
