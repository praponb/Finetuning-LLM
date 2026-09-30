from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy(F.col("end_date_minus_86d").desc(), "id")
w2 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("customer_id", "status").orderBy(F.col("cumulative_monthly_fee").desc_nulls_last())
w5 = Window.partitionBy("customer_id").orderBy("cumulative_monthly_fee", "id")
w6 = Window.partitionBy("status").orderBy("end_date_weekofyear", "id")
w7 = Window.orderBy("cumulative_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("customer_id")
w10 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_subscriptions
    .filter(F.col("end_date") > F.lit("2023-10-31").cast("date"))
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status",
        F.weekofyear("end_date").alias("end_date_weekofyear"),
        F.date_sub("end_date", 86).alias("end_date_minus_86d"),
    )
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

scored = (
    latest
    .withColumn("cumulative_monthly_fee", F.sum("monthly_fee").over(w2))
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w3)), 4))
)

latest_subscriptions = (
    scored
    .withColumn("rank_in_group", F.rank().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

filtered = (
    latest_subscriptions
    .filter((F.col("start_date") > F.lit("2023-11-15").cast("date")) & (F.col("monthly_fee") <= 180))
    .select(
        "*",
        F.round((F.col("cumulative_monthly_fee") * 100), 2).alias("cumulative_monthly_fee_adjusted"),
        F.round((F.col("cumulative_monthly_fee") * 1.15), 2).alias("cumulative_monthly_fee_adjusted_2"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.lag("cumulative_monthly_fee", 1, 0).over(w5).alias("prev_cumulative_monthly_fee"),
        F.lag("monthly_fee_share", 2).over(w6).alias("prev_monthly_fee_share"),
        F.first("cumulative_monthly_fee", ignorenulls=True).over(w7).alias("first_known_cumulative_monthly_fee"),
        F.last("end_date_weekofyear", ignorenulls=True).over(w8).alias("last_known_end_date_weekofyear"),
    )
)

metrics_subscriptions = (
    metrics
    .select(
        "*",
        F.count("cumulative_monthly_fee").over(w9).alias("customer_id_count_cumulative_monthly_fee"),
        F.sum("monthly_fee_share").over(w10).alias("running_monthly_fee_share"),
    )
)

result = (
    metrics_subscriptions
    .select(
        "id",
        "start_date",
        "status",
        "end_date_weekofyear",
        "cumulative_monthly_fee",
        "monthly_fee_share",
        "cumulative_monthly_fee_adjusted",
        "first_known_cumulative_monthly_fee",
        "last_known_end_date_weekofyear",
        F.when(F.col("status").rlike("^(PAUSED|EXPIRED)$"), "PRIORITY").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy(F.col("cumulative_monthly_fee").desc_nulls_first())
)
