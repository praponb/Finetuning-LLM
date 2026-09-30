from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy(F.col("txn_time").desc(), "id")

latest = (
    df_transactions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .rollup("account_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.max("txn_time").alias("ended_at"),
        F.grouping("account_id").alias("grouping_id"),
    )
    .filter(F.col("total_amount") < 0)
)
