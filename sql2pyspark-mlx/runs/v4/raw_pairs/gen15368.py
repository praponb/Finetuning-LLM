from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")
w2 = Window.partitionBy("status").orderBy(F.col("balance").desc())

scored = (
    df_accounts
    .withColumn("prev_balance", F.lag("balance", 1, 0).over(w))
    .withColumn("balance_dense_rank", F.dense_rank().over(w2))
)

result = (
    scored
    .withColumn("balance_dense_rank_tier", F.when(F.col("balance_dense_rank") >= 8, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["status", "balance_dense_rank_tier"], ["status"], []],
        "status",
        "balance_dense_rank_tier",
    )
    .agg(
        F.avg(F.when(F.col("status") == "OPEN", F.col("prev_balance"))).alias("open_avg_prev_balance"),
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("prev_balance").alias("avg_prev_balance"),
        F.grouping("status").alias("grouping_id"),
    )
    .orderBy(F.col("checking_rows").asc_nulls_first())
)
