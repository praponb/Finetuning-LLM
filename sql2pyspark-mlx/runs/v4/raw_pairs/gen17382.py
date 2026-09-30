from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at_weekofyear", "id")
w2 = Window.partitionBy("device").orderBy("started_at_weekofyear", "id")
w3 = Window.orderBy("started_at_weekofyear", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("rank_in_group").desc(), "id")
w5 = Window.partitionBy("device").orderBy("started_at", "id")
w6 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w7 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

valid = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2024-02-26").cast("date")) & F.col("country").isNotNull() & F.col("device").isin(["tablet", "desktop"]))
    .select(
        "*",
        F.weekofyear("started_at").alias("started_at_weekofyear"),
        F.coalesce("browser", F.lit("DIRECT")).alias("resolved_browser"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    latest
    .select(
        "*",
        F.ntile(4).over(w2).alias("started_at_weekofyear_bucket"),
        F.last("browser", ignorenulls=True).over(w3).alias("last_known_browser"),
        F.row_number().over(w4).alias("row_seq"),
        F.lag("started_at_weekofyear", 1).over(w5).alias("prev_started_at_weekofyear"),
        F.ntile(5).over(w2).alias("started_at_weekofyear_bucket_2"),
    )
)

filtered = (
    metrics
    .filter(F.col("started_at") > F.lit("2023-07-16 21:00:00").cast("timestamp"))
    .withColumn("device_country", F.concat("device", F.lit(", "), "country"))
    .withColumn("resolved_browser_2", F.coalesce("browser", F.lit("NONE")))
)

prev_events = filtered.withColumn("prev_started_at", F.lag("started_at").over(w6))

gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("island_id", F.sum("session_start_flag").over(w7))

result = (
    sessions_sessions
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        "started_at_weekofyear",
        "resolved_browser",
        "rank_in_group",
        "started_at_weekofyear_bucket",
        "prev_started_at_weekofyear",
        F.when(F.col("browser").rlike("^(Safari|Edge)$"), "GROUP_B").otherwise("GROUP_A").alias("browser_zone"),
        F.col("min_subscription_monthly_fee"),
    )
    .orderBy(F.col("customer_id").desc_nulls_last())
)
