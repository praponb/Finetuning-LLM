from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "plan").orderBy(F.col("start_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_last())
w3 = Window.partitionBy("customer_id", "status").orderBy(F.col("start_date_day").desc(), "id")
w4 = Window.partitionBy("customer_id").orderBy("start_date_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("plan")
w6 = Window.partitionBy("customer_id", "plan").orderBy(F.col("start_date_day").desc())
w7 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_share").desc_nulls_first(), "id")

filtered = (
    df_subscriptions
    .filter(F.col("end_date") > F.lit("2024-05-30").cast("date"))
    .withColumn("start_date_day", F.dayofmonth("start_date"))
    .withColumn("start_date_minus_83d", F.date_sub("start_date", 83))
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

metrics = (
    ranked
    .select(
        "*",
        F.percent_rank().over(w2).alias("monthly_fee_percent_rank"),
        F.ntile(10).over(w3).alias("start_date_day_bucket"),
        F.sum("monthly_fee").over(w4).alias("cumulative_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w5)), 4).alias("monthly_fee_share"),
    )
)

deduped = (
    metrics
    .withColumn("rank_in_group", F.rank().over(w6))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

deduped_subscriptions = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped_subscriptions
    .select("id", "customer_id", "monthly_fee_percent_rank", "start_date_day_bucket")
    .orderBy(F.col("start_date_day_bucket").desc())
)
