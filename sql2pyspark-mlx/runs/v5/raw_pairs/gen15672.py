from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("customer_id", "status").orderBy("start_date", "id")
w2 = Window.partitionBy("plan", "status").orderBy(F.col("end_date_dayofweek").desc_nulls_last(), "id")
w3 = Window.orderBy("end_date", "id")
w4 = Window.partitionBy("plan", "status").orderBy("end_date_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_subscriptions
    .filter(F.col("end_date") > F.lit("2023-10-12").cast("date"))
    .select("*", F.dayofweek("end_date").alias("end_date_dayofweek"))
)

scored = (
    filtered
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "status",
        "end_date_dayofweek",
        F.lead("end_date_dayofweek").over(w_recent).alias("next_end_date_dayofweek"),
        F.lag("monthly_fee").over(w_recent).alias("prev_monthly_fee"),
        F.lead("monthly_fee").over(w_recent).alias("next_monthly_fee"),
        F.row_number().over(w_recent).alias("event_seq"),
    )
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

metrics = (
    ranked
    .select(
        "id",
        "start_date",
        "status",
        F.lag("next_monthly_fee", 2).over(w3).alias("prev_next_monthly_fee"),
        F.sum("end_date_dayofweek").over(w4).alias("cumulative_end_date_dayofweek"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("prev_next_monthly_fee") >= 198, "PREMIUM").otherwise("STANDARD").alias("prev_next_monthly_fee_tier"),
        F.date_trunc("QUARTER", "start_date").alias("quarter_start"),
    )
    .agg(
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_count"),
        F.percentile_approx("prev_next_monthly_fee", 0.5).alias("prev_next_monthly_fee_p50"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.percentile_approx("prev_next_monthly_fee", 0.9).alias("prev_next_monthly_fee_p90"),
    )
    .filter(F.col("prev_next_monthly_fee_p50") > 0)
    .orderBy(F.col("prev_next_monthly_fee_tier").asc_nulls_last())
)
