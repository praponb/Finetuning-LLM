from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value").orderBy("is_welcome5")
w2 = Window.partitionBy("value").orderBy(F.col("is_welcome5").desc_nulls_last())
w_recent = Window.partitionBy("value")
w4 = Window.partitionBy("value").orderBy(F.col("is_welcome5_cume_dist").desc())
w5 = Window.partitionBy("activity_day").orderBy(F.col("distinct_ids").desc())

exploded = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("value"))

cleaned = (
    exploded
    .filter((F.col("created_at") > F.lit("2025-03-02 03:00:00").cast("timestamp")) & F.col("value").isin(["VIP", "SPRING10", "WELCOME5"]))
    .withColumn("is_welcome5", F.when(F.col("value") == "WELCOME5", 1).otherwise(0))
    .withColumn("created_at_label", F.date_format("created_at", "yyyy-MM"))
    .withColumn("value_filled", F.coalesce("value", F.lit("UNKNOWN")))
)

metrics = (
    cleaned
    .select(
        "*",
        F.cume_dist().over(w).alias("is_welcome5_cume_dist"),
        F.cume_dist().over(w2).alias("is_welcome5_cume_dist_2"),
    )
)

valid = (
    metrics
    .filter(F.col("value").isNotNull() & (F.to_date("created_at") >= "2023-09-23"))
    .select(
        "id",
        "created_at",
        "value",
        "is_welcome5",
        "value_filled",
        "is_welcome5_cume_dist",
        F.date_format("created_at", "yyyy-MM").alias("created_at_label_2"),
        F.coalesce("value", F.lit("DIRECT")).alias("value_filled_2"),
    )
)

cleaned_carts = (
    valid
    .filter(F.col("created_at") > F.lit("2025-03-01").cast("date"))
    .withColumn("value_lower", F.lower("value"))
)

base = (
    cleaned_carts
    .filter(F.col("created_at") > F.lit("2025-03-02").cast("date"))
    .select(
        "id",
        "created_at",
        "value",
        "is_welcome5",
        "is_welcome5_cume_dist",
        "created_at_label_2",
        "value_filled_2",
        "value_lower",
        F.length("value").alias("value_length"),
        F.coalesce("value", F.lit("UNKNOWN")).alias("value_clean"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.max("value_length").over(w_recent).alias("value_max_value_length"),
        F.percent_rank().over(w4).alias("is_welcome5_cume_dist_percent_rank"),
        F.max("is_welcome5").over(w_recent).alias("value_max_is_welcome5"),
    )
)

result = (
    enriched
    .groupBy(
        F.date_trunc("DAY", "created_at").alias("activity_day"),
        F.when(F.col("value_max_value_length") >= 88, "LARGE").otherwise("SMALL").alias("value_max_value_length_tier"),
        "value",
    )
    .agg(
        F.count(F.when(F.col("value") == "SPRING10", 1)).alias("spring10_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .withColumn("distinct_ids_percent_rank", F.percent_rank().over(w5))
    .orderBy(F.col("value").desc_nulls_first())
)
