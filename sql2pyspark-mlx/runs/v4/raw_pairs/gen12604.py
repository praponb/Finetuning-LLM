from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("browser").orderBy(F.col("subscription_id").desc_nulls_first())
w_running = Window.partitionBy("customer_id").orderBy("started_at", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running_2 = Window.partitionBy("country").orderBy(F.col("monthly_fee").desc(), "id", "subscription_id")
w4 = Window.partitionBy("customer_id", "browser")
w5 = Window.partitionBy("country").orderBy(F.col("subscription_id_2").desc_nulls_first(), "id", "subscription_id", "subscription_id_2")

filtered = (
    df_sessions
    .filter(~F.col("country").isin(["SG", "UK", "US"]))
    .select(
        "*",
        F.trim("browser").alias("clean_browser"),
        F.col("started_at").cast("date").alias("started_at_day"),
    )
)

attributed = (
    filtered
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.started_at") >= F.col("sub.start_date")) & (F.col("src.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

enriched = (
    attributed
    .withColumn("subscription_id_rank", F.rank().over(w_grp))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w_running))
    .withColumn("seq_no", F.row_number().over(w_running_2))
    .withColumn("subscription_id_share", F.round((F.col("subscription_id") / F.sum("subscription_id").over(w4)), 4))
)

with_window = (
    enriched
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.started_at") >= F.col("sub.start_date")) & (F.col("a.started_at") <= F.date_add(F.col("sub.end_date"), 10)), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

base = (
    with_window
    .filter(F.col("country").isin(["SG", "UK"]))
    .select(
        "*",
        F.round("monthly_fee", 0).alias("monthly_fee_rounded"),
        F.round((F.col("subscription_id_share") * 1.07), 2).alias("subscription_id_share_scaled"),
    )
)

ranked = (
    base
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .select(
        "id",
        "device",
        "started_at",
        "country",
        "clean_browser",
        "started_at_day",
        "subscription_id",
        "monthly_fee",
        "subscription_id_rank",
    )
    .orderBy(F.col("started_at").desc_nulls_first())
)
