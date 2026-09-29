from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy(F.col("start_date").desc_nulls_first(), "id")
w_ordered = Window.partitionBy("status")
w3 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").asc_nulls_first())
w4 = Window.orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("start_date", "id")
w6 = Window.partitionBy("plan", "status")
w7 = Window.partitionBy("status").orderBy("status_count_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status", "customer_id").orderBy("monthly_fee_share", "id").rowsBetween(-5, Window.currentRow)
w9 = Window.partitionBy("plan").orderBy("monthly_fee_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("customer_id")
w11 = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("customer_id", "status")

ranked = (
    df_subscriptions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

scored = (
    ranked
    .select(
        "*",
        F.count("monthly_fee").over(w_ordered).alias("status_count_monthly_fee"),
        F.dense_rank().over(w3).alias("monthly_fee_dense_rank"),
        F.sum("monthly_fee").over(w4).alias("cumulative_monthly_fee"),
    )
)

scored_subscriptions = (
    scored
    .withColumn("status_min_monthly_fee", F.min("monthly_fee").over(w_ordered))
    .withColumn("event_seq", F.row_number().over(w5))
    .withColumn("status_count_monthly_fee_share", F.round((F.col("status_count_monthly_fee") / F.sum("status_count_monthly_fee").over(w6)), 4))
)

scored_subscriptions_2 = (
    scored_subscriptions
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w_ordered)), 4))
    .withColumn("cumulative_event_seq", F.sum("event_seq").over(w7))
)

result = (
    scored_subscriptions_2
    .select(
        "*",
        F.avg("status_min_monthly_fee").over(w8).alias("status_min_monthly_fee_avg_5"),
        F.sum("status_count_monthly_fee_share").over(w9).alias("cumulative_status_count_monthly_fee_share"),
        F.stddev_samp("monthly_fee_dense_rank").over(w10).alias("customer_id_stddev_samp_monthly_fee_dense_rank"),
        F.first("monthly_fee_share", ignorenulls=True).over(w11).alias("first_known_monthly_fee_share"),
        F.round((F.col("cumulative_monthly_fee") / F.sum("cumulative_monthly_fee").over(w12)), 4).alias("cumulative_monthly_fee_share"),
    )
)
