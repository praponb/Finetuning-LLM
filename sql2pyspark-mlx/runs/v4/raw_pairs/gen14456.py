from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount")
w2 = Window.partitionBy("account_id").orderBy("amount", "id")
w3 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.partitionBy("merchant", "account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("merchant", "account_id").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)

metrics = (
    df_transactions
    .select(
        "*",
        F.percent_rank().over(w).alias("amount_percent_rank"),
        F.lead("amount", 2).over(w2).alias("next_amount"),
        F.sum("amount").over(w3).alias("amount_sum_30d"),
        F.max("amount").over(w4).alias("amount_max_30d"),
        F.avg("amount").over(w5).alias("amount_avg_6"),
    )
)

result = (
    metrics
    .groupBy("account_id")
    .agg(
        F.max("txn_time").alias("ended_at"),
        F.max("txn_time").alias("last_seen"),
        F.count_if(F.col("merchant") == "7-Eleven").alias("7_eleven_events"),
        F.min("amount_avg_6").alias("min_amount_avg_6"),
        F.count_if(F.col("merchant") == "Starbucks").alias("starbucks_events"),
    )
    .orderBy("starbucks_events")
)
