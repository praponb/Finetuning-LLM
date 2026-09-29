from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_transactions
    .select(
        "*",
        F.avg("amount").over(w_hist).alias("running_avg_amount"),
        F.min("amount").over(w_hist).alias("running_min_amount"),
        F.sum("amount").over(w_hist).alias("running_sum_amount"),
        F.count("amount").over(w_hist).alias("running_count_amount"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("running_avg_amount") >= 3100, "LARGE").otherwise("SMALL").alias("running_avg_amount_tier"),
    )
    .agg(
        F.countDistinct("account_id").alias("distinct_account_ids"),
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("running_sum_amount")).otherwise(0)).alias("starbucks_running_sum_amount"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
    )
    .orderBy(F.col("distinct_account_ids").desc_nulls_first())
)
