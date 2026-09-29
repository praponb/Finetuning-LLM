from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy(F.col("monthly_fee").desc())
w_grp = Window.partitionBy("customer_id", "plan").orderBy("end_date", "id")
w_running = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("end_date_period", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.orderBy(F.col("monthly_fee").desc_nulls_first())

deduped = (
    df_subscriptions
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "customer_id", "plan", "end_date", "monthly_fee", "status")
)

cleaned = (
    deduped
    .filter((F.col("end_date") > F.lit("2023-09-14").cast("date")) & F.col("customer_id").isNotNull())
    .select("*", F.date_trunc("YEAR", "end_date").alias("end_date_period"))
)

enriched = (
    cleaned
    .select(
        "id",
        "customer_id",
        "plan",
        "monthly_fee",
        "end_date_period",
        F.row_number().over(w_grp).alias("event_seq"),
        F.last("monthly_fee", ignorenulls=True).over(w_running).alias("last_known_monthly_fee"),
        F.avg("monthly_fee").over(w4).alias("monthly_fee_avg_5"),
        F.percent_rank().over(w5).alias("monthly_fee_percent_rank"),
    )
)

matched_keys = (
    enriched
    .select("id", "customer_id")
    .subtract(
        df_subscriptions
        .filter(F.col("monthly_fee") < 220)
        .select("id", "customer_id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("monthly_fee_avg_5") != 202)
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date_period",
        "event_seq",
        "last_known_monthly_fee",
        "monthly_fee_avg_5",
        "monthly_fee_percent_rank",
        F.substring("plan", 1, 2).alias("plan_prefix"),
    )
)

result = (
    valid
    .alias("a")
    .join(df_customers.alias("dim"), F.col("a.customer_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["country", "segment", "plan"], ["country", "segment"], ["country"]],
        F.col("dim.country").alias("country"),
        F.col("dim.segment").alias("segment"),
        F.col("a.plan").alias("plan"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("a.monthly_fee_avg_5")).alias("total_monthly_fee_avg_5"),
    )
)
