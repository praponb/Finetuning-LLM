from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_last())
w3 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(-3, -1)
w4 = Window.orderBy("start_date", "id")
w5 = Window.partitionBy("customer_id").orderBy("monthly_fee")

with_windows = (
    df_subscriptions
    .select(
        "*",
        F.avg("monthly_fee").over(w).alias("monthly_fee_avg_2"),
        F.dense_rank().over(w2).alias("monthly_fee_dense_rank"),
        F.avg("monthly_fee").over(w3).alias("monthly_fee_avg_3_prior"),
        F.row_number().over(w4).alias("seq_no"),
        F.rank().over(w5).alias("monthly_fee_rank"),
    )
)

result = (
    with_windows
    .withColumn("monthly_fee_avg_2_tier", F.when(F.col("monthly_fee_avg_2") >= 262, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["seq_no", "monthly_fee_avg_2_tier"], ["seq_no"], []],
        "seq_no",
        "monthly_fee_avg_2_tier",
    )
    .agg(
        F.count_if(F.col("status") == "PAUSED").alias("paused_events"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("paused_events") >= 1)
)
