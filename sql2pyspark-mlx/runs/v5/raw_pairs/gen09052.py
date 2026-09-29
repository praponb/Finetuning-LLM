from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("end_date_minus_82d").desc_nulls_last(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("end_date_minus_82d").asc_nulls_first(), "id")

base = (
    df_subscriptions
    .filter(F.col("end_date") > F.to_date(F.lit("2023-08-26")))
    .select("*", F.date_sub("end_date", 82).alias("end_date_minus_82d"))
)

latest = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

deduped = (
    latest
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

with_window = (
    deduped
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.end_date") >= F.col("sub.start_date")) & (F.col("src.end_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

result = (
    with_window
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "latest_rank",
        "subscription_id",
        "subscription_monthly_fee",
    )
)
