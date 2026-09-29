from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("plan", "customer_id").orderBy("end_date", "id").rowsBetween(-2, -1)
w3 = Window.orderBy(F.col("monthly_fee").asc_nulls_last())
w4 = Window.partitionBy("customer_id", "status").orderBy("monthly_fee_avg_2_prior", "id")
w_grp = Window.partitionBy("plan").orderBy("end_date", "id")
w6 = Window.orderBy(F.col("prev_monthly_fee").desc_nulls_last())
w7 = Window.partitionBy("plan", "customer_id").orderBy("monthly_fee_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(110, 150))
    .select("*", F.trim("plan").alias("clean_plan"))
)

metrics = (
    cleaned
    .withColumn("monthly_fee_avg_3", F.avg("monthly_fee").over(w))
    .withColumn("monthly_fee_avg_2_prior", F.avg("monthly_fee").over(w2))
)

with_windows = (
    metrics
    .select(
        "*",
        F.rank().over(w3).alias("monthly_fee_rank"),
        F.lead("monthly_fee_avg_3", 1).over(w4).alias("next_monthly_fee_avg_3"),
        F.lag("monthly_fee", 2, 0).over(w_grp).alias("prev_monthly_fee"),
    )
)

metrics_subscriptions = (
    with_windows
    .withColumn("prev_monthly_fee_percent_rank", F.percent_rank().over(w6))
    .withColumn("cumulative_next_monthly_fee_avg_3", F.sum("next_monthly_fee_avg_3").over(w7))
)

result = (
    metrics_subscriptions
    .groupBy("customer_id")
    .agg(
        F.round(F.avg("monthly_fee_rank"), 2).alias("avg_monthly_fee_rank"),
        F.round(F.avg("monthly_fee_rank"), 2).alias("avg_monthly_fee_rank_2"),
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_rows"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
    .filter(F.col("num_plans") > 1)
)
