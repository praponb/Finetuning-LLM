from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("plan").orderBy("days_end_date_to_start_date", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("last_known_monthly_fee").desc_nulls_first(), "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("last_known_monthly_fee").desc(), "id")
w5 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_avg_3").desc_nulls_last(), "id")

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & F.col("status").isin(["CANCELLED", "ACTIVE", "EXPIRED"]))
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        F.datediff("start_date", "end_date").alias("days_end_date_to_start_date"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "customer_id",
        "plan",
        "status",
        F.last("monthly_fee", ignorenulls=True).over(w).alias("last_known_monthly_fee"),
        F.avg("monthly_fee").over(w_recent).alias("monthly_fee_avg_3"),
    )
)

deduped = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

ranked = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "plan", "status", "monthly_fee_avg_3", "latest_rank")
)

deduped_subscriptions = (
    ranked
    .withColumn("latest_rank_2", F.row_number().over(w5))
    .filter(F.col("latest_rank_2") == 1)
)

cleaned = (
    deduped_subscriptions
    .filter(F.col("monthly_fee_avg_3") == 175)
    .select(
        "id",
        "plan",
        "status",
        "monthly_fee_avg_3",
        "latest_rank",
        F.round("monthly_fee_avg_3", 0).alias("monthly_fee_avg_3_rounded"),
        F.concat("status", F.lit(", "), "plan").alias("status_plan"),
    )
)

result = (
    cleaned
    .groupBy("plan")
    .agg(
        F.percentile_approx("monthly_fee_avg_3_rounded", 0.95).alias("monthly_fee_avg_3_rounded_p95"),
        F.percentile_approx("monthly_fee_avg_3", 0.5).alias("monthly_fee_avg_3_p50"),
    )
)
