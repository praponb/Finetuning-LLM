from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee", "id")
w_grp = Window.partitionBy("plan", "customer_id").orderBy("start_date", "id")
w3 = Window.partitionBy("plan", "customer_id").orderBy("latest_rank", "id")
w4 = Window.partitionBy("plan").orderBy(F.col("latest_rank").desc(), "id")

latest = (
    df_subscriptions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.lag("monthly_fee").over(w_grp).alias("prev_monthly_fee"),
        F.lead("monthly_fee").over(w_grp).alias("next_monthly_fee"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

with_dims = (
    scored
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select("s.*", F.col("c.name"))
)

latest_subscriptions = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

cleaned = (
    latest_subscriptions
    .filter(F.col("end_date") > F.to_date(F.lit("2023-06-18")))
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        "latest_rank",
        "prev_monthly_fee",
        "next_monthly_fee",
        "event_seq",
        "dedup_rank",
        F.round((F.col("next_monthly_fee") * 100), 2).alias("next_monthly_fee_scaled"),
    )
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

pivoted = (
    deduped
    .groupBy("customer_id")
    .pivot("status", ["EXPIRED", "CANCELLED"])
    .agg(F.avg("next_monthly_fee_scaled"))
)

result = (
    pivoted
    .select("customer_id", "CANCELLED")
    .orderBy(F.col("CANCELLED").desc(), "customer_id")
    .limit(20)
)
