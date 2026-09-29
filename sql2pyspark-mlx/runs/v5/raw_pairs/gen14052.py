from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("starts_on", "id")
w2 = Window.orderBy(F.col("starts_on_year").desc(), "id")
w3 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("starts_on_year", "id")
w5 = Window.partitionBy("channel")
w6 = Window.orderBy("starts_on_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-04-08").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        F.trim("promo_code").alias("clean_promo_code"),
        F.year("starts_on").alias("starts_on_year"),
    )
)

valid = (
    base
    .filter(F.col("channel").isNotNull() & (F.col("ends_on") <= "2023-03-01"))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "clean_promo_code",
        "starts_on_year",
        F.initcap(F.trim("channel")).alias("channel_title"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

metrics = (
    valid
    .filter(F.col("starts_on") >= "2023-10-01")
    .select(
        "*",
        F.lag("starts_on_year", 1).over(w).alias("prev_starts_on_year"),
        F.ntile(10).over(w2).alias("starts_on_year_bucket"),
    )
)

metrics_promotions = (
    metrics
    .withColumn("first_known_starts_on_year_bucket", F.first("starts_on_year_bucket", ignorenulls=True).over(w3))
    .withColumn("starts_on_year_bucket_2", F.ntile(3).over(w4))
    .withColumn("channel_avg_starts_on_year_bucket", F.avg("starts_on_year_bucket").over(w5))
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w6))
)

result = (
    metrics_promotions
    .groupBy(
        F.when(F.col("starts_on_year") >= 63, "HIGH").otherwise("LOW").alias("starts_on_year_tier"),
        "channel",
    )
    .agg(
        F.sum(F.when(F.col("channel") == "partner", F.col("starts_on_year_bucket_2")).otherwise(0)).alias("partner_starts_on_year_bucket_2"),
        F.round(F.sum("prev_starts_on_year") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("prev_starts_on_year_per_mobile"),
        F.percentile_approx("starts_on_year_bucket_2", [0.25, 0.5, 0.75]).alias("starts_on_year_bucket_2_quartiles"),
        F.max("prev_starts_on_year").alias("max_prev_starts_on_year"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.sum(F.when(F.col("channel") == "store", F.col("channel_avg_starts_on_year_bucket")).otherwise(0)).alias("store_channel_avg_starts_on_year_bucket"),
    )
    .filter(F.col("prev_starts_on_year_per_mobile") >= 0)
)
