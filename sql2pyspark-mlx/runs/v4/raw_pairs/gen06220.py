from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id", "subscription_id", "subscription_id_2")
w2 = Window.partitionBy("browser").orderBy("started_at", "id", "subscription_id", "subscription_id_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("subscription_id_2_tier").orderBy(F.col("subscription_monthly_fee_3_per_basic").desc())

cleaned = (
    df_sessions
    .filter(F.col("browser").isNotNull())
    .withColumn("device_upper", F.upper("device"))
    .withColumn("country_device", F.concat("country", F.lit(", "), "device"))
)

with_window = (
    cleaned
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.started_at") >= F.col("sub.start_date")) & (F.col("s.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_window_sessions = (
    with_window
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & F.col("s.started_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

gaps = (
    with_window_sessions
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

attributed = (
    sessions_sessions
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.started_at") >= F.col("sub.start_date")) & (F.col("src.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id_3"),
        F.col("sub.plan").alias("subscription_plan_2"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee_2"),
    )
)

attributed_sessions = (
    attributed
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.started_at") >= F.col("sub.start_date")) & (F.col("s.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id_4"),
        F.col("sub.plan").alias("subscription_plan_3"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee_3"),
    )
)

matched = (
    attributed_sessions
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.started_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id_5"),
        F.col("sub.plan").alias("subscription_plan_4"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee_4"),
    )
)

totals = (
    matched
    .groupBy(
        F.when(F.col("subscription_id_2") >= 10, "LARGE").otherwise("SMALL").alias("subscription_id_2_tier"),
        F.when(F.col("monthly_fee") >= 246, "HIGH").otherwise("LOW").alias("monthly_fee_tier"),
    )
    .agg(
        F.max("started_at").alias("ended_at"),
        F.round(F.sum("subscription_monthly_fee_3") / F.nullif(F.count(F.when(F.col("subscription_plan_3") == "basic", 1)), F.lit(0)), 2).alias("subscription_monthly_fee_3_per_basic"),
        F.sort_array(F.collect_set("subscription_plan_3")).alias("subscription_plan_3_list"),
    )
    .withColumn("subscription_monthly_fee_3_per_basic_dense_rank", F.dense_rank().over(w3))
)

result = (
    totals
    .select(
        "subscription_id_2_tier",
        "monthly_fee_tier",
        "ended_at",
        "subscription_monthly_fee_3_per_basic",
        "subscription_monthly_fee_3_per_basic_dense_rank",
    )
    .orderBy(F.col("ended_at").desc_nulls_last())
)
