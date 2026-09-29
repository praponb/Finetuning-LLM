from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy(F.col("opened_date").desc_nulls_last(), "id")

ranked = (
    df_accounts
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    ranked
    .groupBy(
        "customer_id",
        F.when(F.col("balance") >= 82000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance")).otherwise(0)).alias("frozen_balance"),
        F.stddev_samp("balance").alias("balance_stddev"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
)
