from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("status").orderBy("monthly_fee", "id")
w3 = Window.partitionBy("status").orderBy(F.col("next_monthly_fee").desc())
w4 = Window.orderBy("monthly_fee_bucket", "id")

cleaned = (
    df_subscriptions
    .filter(F.col("status").isin(["EXPIRED", "ACTIVE", "PAUSED"]))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.last_day("end_date").alias("end_date_month_end"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.lead("monthly_fee").over(w).alias("next_monthly_fee"),
        F.row_number().over(w).alias("event_seq"),
        F.ntile(4).over(w2).alias("monthly_fee_bucket"),
    )
)

enriched_subscriptions = (
    enriched
    .filter(F.col("next_monthly_fee").isNotNull())
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        "status",
        "end_date_month_end",
        "next_monthly_fee",
        F.rank().over(w3).alias("next_monthly_fee_rank"),
        F.row_number().over(w4).alias("row_seq"),
        F.rank().over(w3).alias("next_monthly_fee_rank_2"),
    )
)

filtered = (
    enriched_subscriptions
    .filter(F.year("end_date") == 2024)
    .select("*", F.ceil("next_monthly_fee_rank").alias("next_monthly_fee_rank_whole"))
)

by_type = (
    filtered
    .groupBy("customer_id")
    .pivot("status", ["EXPIRED", "ACTIVE"])
    .agg(F.sum("monthly_fee"))
    .select(
        "customer_id",
        F.col("EXPIRED").alias("expired"),
        F.col("ACTIVE").alias("active"),
    )
)

result = (
    by_type
    .select("customer_id", "expired")
    .orderBy(F.col("customer_id").desc_nulls_last())
)
