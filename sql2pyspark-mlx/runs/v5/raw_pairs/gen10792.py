from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("subscription_id").asc_nulls_first())
w2 = Window.partitionBy("plan").orderBy("subscription_id")

cleaned = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-03-13").cast("date")) & ~F.col("status").isin(["CANCELLED", "PAUSED", "ACTIVE"]))
    .withColumn("end_date_plus_12d", F.date_add("end_date", 12))
)

matched = (
    cleaned
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.end_date_plus_12d") >= F.col("sub.start_date")) & (F.col("a.end_date_plus_12d") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

enriched = (
    matched
    .select(
        "id",
        "customer_id",
        "end_date",
        "status",
        "end_date_plus_12d",
        "subscription_id",
        F.cume_dist().over(w).alias("subscription_id_cume_dist"),
        F.cume_dist().over(w2).alias("subscription_id_cume_dist_2"),
    )
)

with_window = (
    enriched
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.end_date_plus_12d") >= F.col("sub.start_date")) & (F.col("a.end_date_plus_12d") <= F.date_add(F.col("sub.end_date"), 7)), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    with_window
    .alias("e")
    .join(df_customers.alias("dim"), F.col("e.customer_id") == F.col("dim.id"), "inner")
    .groupBy(
        F.col("dim.country").alias("country"),
        F.col("dim.segment").alias("segment"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("e.subscription_id_2")).alias("max_subscription_id_2"),
    )
)
