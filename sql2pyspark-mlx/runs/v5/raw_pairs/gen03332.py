from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code")
w3 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code").orderBy("starts_on", "id")

scored = (
    df_promotions
    .select(
        "*",
        F.last("channel", ignorenulls=True).over(w).alias("last_known_channel"),
        F.stddev_samp("discount_pct").over(w2).alias("promo_code_stddev_samp_discount_pct"),
        F.last("channel", ignorenulls=True).over(w3).alias("last_known_channel_2"),
        F.lead("discount_pct", 2).over(w4).alias("next_discount_pct"),
    )
)

cleaned = (
    scored
    .filter(F.col("starts_on") > F.lit("2023-07-04").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "promo_code_stddev_samp_discount_pct",
        "last_known_channel_2",
        "next_discount_pct",
        F.date_format("ends_on", "MMM yyyy").alias("ends_on_label"),
    )
)

base = (
    cleaned
    .filter((F.col("next_discount_pct") >= 0.26) & (F.col("starts_on") > F.lit("2023-04-02").cast("date")))
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "last_known_channel_2",
        "next_discount_pct",
        "ends_on_label",
        F.datediff(F.current_date(), "starts_on").alias("days_since_starts_on"),
    )
)

valid = (
    base
    .filter(F.col("ends_on") > F.lit("2023-08-11").cast("date"))
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "ends_on_label",
        "days_since_starts_on",
        F.add_months("starts_on", 12).alias("starts_on_plus_months"),
    )
)

filtered = (
    valid
    .filter((F.col("starts_on") > F.lit("2023-04-04").cast("date")) & (F.col("promo_code") == "SPRING10"))
    .withColumn("ends_on_weekofyear", F.weekofyear("ends_on"))
)

result = (
    filtered
    .groupBy("promo_code")
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.avg("days_since_starts_on").alias("avg_days_since_starts_on"),
        F.percentile_approx("days_since_starts_on", 0.9).alias("days_since_starts_on_p90"),
        F.sum(F.when(F.col("promo_code") == "WELCOME5", F.col("ends_on_weekofyear")).otherwise(0)).alias("welcome5_ends_on_weekofyear"),
        F.round(F.avg("days_since_starts_on"), 2).alias("avg_days_since_starts_on_2"),
    )
    .orderBy(F.col("promo_code").asc_nulls_last())
)
