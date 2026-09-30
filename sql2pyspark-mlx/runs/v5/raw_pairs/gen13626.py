from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id")
w2 = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("rank_in_group", "id")
w5 = Window.partitionBy("status").orderBy("start_date", "id")
w6 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc(), "id")
w7 = Window.partitionBy("plan", "status").orderBy("prev_monthly_fee_2", "id")
w8 = Window.partitionBy("plan").orderBy("monthly_fee_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_subscriptions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    deduped
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "rank_in_group",
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_5"),
        F.sum("monthly_fee").over(w3).alias("cumulative_monthly_fee"),
        F.lag("monthly_fee", 2, 0).over(w4).alias("prev_monthly_fee"),
        F.lag("monthly_fee", 1, 0).over(w5).alias("prev_monthly_fee_2"),
        F.ntile(4).over(w6).alias("monthly_fee_bucket"),
    )
)

scored_subscriptions = (
    scored
    .withColumn("next_monthly_fee_avg_5", F.lead("monthly_fee_avg_5").over(w7))
    .withColumn("next_monthly_fee", F.lead("monthly_fee").over(w7))
    .withColumn("event_seq", F.row_number().over(w7))
    .withColumn("cumulative_cumulative_monthly_fee", F.sum("cumulative_monthly_fee").over(w8))
)

cleaned = (
    scored_subscriptions
    .filter(F.col("monthly_fee_bucket") >= 6)
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        "rank_in_group",
        "monthly_fee_avg_5",
        "cumulative_monthly_fee",
        "monthly_fee_bucket",
        "next_monthly_fee_avg_5",
        "next_monthly_fee",
        "cumulative_cumulative_monthly_fee",
        (F.col("monthly_fee") - F.col("monthly_fee_avg_5")).alias("monthly_fee_minus_monthly_fee_avg_5"),
    )
)

report = (
    cleaned
    .select(
        "id",
        "plan",
        "monthly_fee",
        "rank_in_group",
        "cumulative_monthly_fee",
        "monthly_fee_bucket",
        "next_monthly_fee_avg_5",
        "next_monthly_fee",
        "cumulative_cumulative_monthly_fee",
    )
    .orderBy("id")
)

result = (
    report
    .select(
        "id",
        "rank_in_group",
        "cumulative_monthly_fee",
        "monthly_fee_bucket",
        "next_monthly_fee_avg_5",
        "next_monthly_fee",
        "cumulative_cumulative_monthly_fee",
    )
    .orderBy(F.col("next_monthly_fee_avg_5").desc_nulls_last())
)
