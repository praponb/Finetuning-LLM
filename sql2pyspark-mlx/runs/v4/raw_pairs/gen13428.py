from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("monthly_fee").desc())
w2 = Window.partitionBy("device", "country").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.orderBy("monthly_fee", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device").orderBy(F.col("subscription_id").desc(), "id", "subscription_id")

filtered = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-07-10").cast("date")) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.initcap(F.trim("browser")).alias("browser_title"),
        F.length("device").alias("device_length"),
    )
)

with_window = (
    filtered
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.started_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

latest = (
    with_window
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.sum("monthly_fee").over(w2).alias("monthly_fee_sum_90d"),
        F.first("country", ignorenulls=True).over(w3).alias("first_known_country"),
        F.ntile(3).over(w4).alias("subscription_id_bucket"),
    )
)

grouped = (
    scored
    .groupBy(
        F.when(F.col("subscription_id") >= 9, "HIGH").otherwise("LOW").alias("subscription_id_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.sum(F.when(F.col("plan") == "standard", F.col("subscription_id_bucket")).otherwise(0)).alias("standard_subscription_id_bucket"),
        F.max("started_at").alias("ended_at"),
    )
)

result = (
    grouped
    .select("subscription_id_tier", "plan_list", "standard_subscription_id_bucket")
    .orderBy("subscription_id_tier")
)
