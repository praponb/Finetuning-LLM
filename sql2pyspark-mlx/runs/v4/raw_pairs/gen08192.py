from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy("start_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("is_active").desc())
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("status").orderBy(F.col("first_known_monthly_fee").desc_nulls_first(), "id")

valid = (
    df_subscriptions
    .filter((F.col("end_date") >= F.to_date(F.lit("2023-05-11"))) & F.col("status").isin(["CANCELLED", "PAUSED", "EXPIRED"]))
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.when(F.col("status") == "ACTIVE", 1).otherwise(0).alias("is_active"),
    )
)

valid_subscriptions = (
    valid
    .filter(F.col("end_date") > F.lit("2023-10-15").cast("date"))
    .withColumn("start_date_plus_months", F.add_months("start_date", 3))
)

scored = (
    valid_subscriptions
    .select(
        "*",
        F.first("monthly_fee", ignorenulls=True).over(w).alias("first_known_monthly_fee"),
        F.percent_rank().over(w2).alias("is_active_percent_rank"),
        F.sum("is_active").over(w3).alias("status_sum_is_active"),
    )
)

ranked = (
    scored
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .withColumn("quarter_start", F.date_trunc("QUARTER", "start_date_plus_months"))
    .withColumn("status_sum_is_active_tier", F.when(F.col("status_sum_is_active") >= 428, "LARGE").otherwise("SMALL"))
    .withColumn("week_start", F.date_trunc("WEEK", "start_date"))
    .groupingSets(
        [["quarter_start", "status_sum_is_active_tier", "week_start"], ["quarter_start", "status_sum_is_active_tier"], []],
        "quarter_start",
        "status_sum_is_active_tier",
        "week_start",
    )
    .agg(
        F.round(F.avg("is_active_percent_rank"), 2).alias("avg_is_active_percent_rank"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("status") == "PAUSED", 1)), F.lit(0)), 2).alias("monthly_fee_per_paused"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.avg(F.when(F.col("status") == "CANCELLED", F.col("first_known_monthly_fee"))).alias("cancelled_avg_first_known_monthly_fee"),
    )
    .orderBy("status_sum_is_active_tier")
)
