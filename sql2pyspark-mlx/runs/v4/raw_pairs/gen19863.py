from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "plan").orderBy(F.col("monthly_fee").asc_nulls_first(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")
w3 = Window.partitionBy("status").orderBy("rn", "id")
w4 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("plan").orderBy(F.col("age").desc(), "id")

deduped = (
    df_subscriptions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    deduped
    .withColumn("monthly_fee_bucket", F.ntile(5).over(w2))
    .withColumn("next_monthly_fee", F.lead("monthly_fee", 2, 0).over(w3))
    .withColumn("monthly_fee_avg_2", F.avg("monthly_fee").over(w4))
)

enriched_subscriptions = (
    enriched
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.city"), F.col("d.age"))
)

cleaned = (
    enriched_subscriptions
    .filter(F.col("end_date") > F.lit("2023-12-01").cast("date"))
    .select(
        "*",
        F.trim("status").alias("clean_status"),
        F.substring("status", 1, 2).alias("status_prefix"),
    )
)

ranked = (
    cleaned
    .withColumn("rn_2", F.row_number().over(w5))
    .filter(F.col("rn_2") == 1)
)

with_window = (
    ranked
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.start_date") >= F.col("sub.start_date")) & (F.col("s.start_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

base = (
    with_window
    .filter((F.col("end_date") > F.to_date(F.lit("2024-04-18"))) & F.col("subscription_plan").isNotNull())
    .withColumn("subscription_monthly_fee_plus_rn_2", (F.col("subscription_monthly_fee") + F.col("rn_2")))
)

result = (
    base
    .groupBy("status")
    .agg(
        F.avg("monthly_fee_avg_2").alias("avg_monthly_fee_avg_2"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("avg_monthly_fee_avg_2") > 0)
)
