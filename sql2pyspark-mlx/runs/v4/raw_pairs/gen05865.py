from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser")
w_recent = Window.partitionBy("country").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

valid = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.date_trunc("year", "started_at").alias("started_at_period"),
        F.coalesce("browser", F.lit("OTHER")).alias("resolved_browser"),
    )
)

remaining_keys = (
    valid
    .select("customer_id", "country")
    .intersectAll(
        df_sessions
        .filter(F.to_date("started_at") < "2025-08-01")
        .select("customer_id", "country")
    )
)

verified = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.email"), F.col("d.name"), F.col("d.age"))
)

with_windows = (
    joined
    .withColumn("browser_stddev_samp_age", F.stddev_samp("age").over(w))
    .withColumn("age_avg_30d", F.avg("age").over(w_recent))
)

result = (
    with_windows
    .groupBy("customer_id")
    .pivot("browser", ["Chrome", "Firefox"])
    .agg(
        F.sum("browser_stddev_samp_age").alias("value_sum"),
        F.count(F.lit(1)).alias("item_count"),
    )
    .select(
        "customer_id",
        F.col("Chrome_value_sum").alias("chrome_value_sum"),
        F.col("Chrome_item_count").alias("chrome_item_count"),
        F.col("Firefox_value_sum").alias("firefox_value_sum"),
        F.col("Firefox_item_count").alias("firefox_item_count"),
    )
)
