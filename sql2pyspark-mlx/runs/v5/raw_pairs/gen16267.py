from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("created_at_hour")

exploded = (
    df_carts
    .select("id", "created_at", F.explode_outer("coupon_codes").alias("element"))
)

filtered = (
    exploded
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-02"))) & (F.col("created_at") < "2024-05-01"))
    .select(
        "*",
        F.hour("created_at").alias("created_at_hour"),
        F.when(F.col("element").isin(["SPRING10", "WELCOME5"]), "Tier 1").otherwise("Other").alias("element_group"),
        F.coalesce("element", F.lit("DIRECT")).alias("element_clean"),
    )
)

scored = filtered.withColumn("created_at_hour_cume_dist", F.cume_dist().over(w))

filtered_carts = (
    scored
    .filter((F.col("created_at") > F.lit("2025-03-01 16:00:00").cast("timestamp")) & F.col("element").isNotNull())
    .select(
        "id",
        "created_at",
        "element",
        "element_group",
        "created_at_hour_cume_dist",
        (F.col("created_at_hour") / F.nullif("created_at_hour_cume_dist", F.lit(0))).alias("created_at_hour_per_created_at_hour_cume_dist"),
        F.coalesce("element", F.lit("DIRECT")).alias("resolved_element"),
    )
)

cleaned = (
    filtered_carts
    .filter(F.col("created_at") >= F.lit("2025-03-01 11:00:00").cast("timestamp"))
    .select(
        "id",
        "created_at",
        "element",
        "created_at_hour_cume_dist",
        "created_at_hour_per_created_at_hour_cume_dist",
        F.col("created_at").cast("date").alias("created_at_day"),
    )
)

cleaned_carts = (
    cleaned
    .filter(F.col("created_at_hour_per_created_at_hour_cume_dist") < 24)
    .select(
        "id",
        "created_at",
        "element",
        "created_at_hour_per_created_at_hour_cume_dist",
        "created_at_day",
        (F.col("created_at_hour_per_created_at_hour_cume_dist") - F.col("created_at_hour_cume_dist")).alias("created_at_hour_per_created_at_hour_cume_dist_minus_created_at_hour_cume_dist"),
        F.coalesce("element", F.lit("NONE")).alias("element_filled"),
    )
)

result = (
    cleaned_carts
    .groupBy(
        F.date_trunc("day", "created_at").alias("activity_day"),
        F.when(F.col("created_at_hour_per_created_at_hour_cume_dist_minus_created_at_hour_cume_dist") >= 81, "LARGE").otherwise("SMALL").alias("created_at_hour_per_created_at_hour_cume_dist_minus_created_at_hour_cume_dist_tier"),
        F.date_trunc("month", "created_at_day").alias("activity_month"),
    )
    .agg(
        F.count(F.when(F.col("element") == "BUNDLE", 1)).alias("bundle_count"),
        F.round(F.avg("created_at_hour_per_created_at_hour_cume_dist_minus_created_at_hour_cume_dist"), 2).alias("avg_created_at_hour_per_created_at_hour_cume_dist_minus_created_at_hour_cume_dist"),
        F.sort_array(F.collect_set("element")).alias("element_list"),
        F.size(F.collect_set("element")).alias("num_elements"),
        F.sum(F.when(F.col("element") == "SPRING10", F.col("created_at_hour_per_created_at_hour_cume_dist_minus_created_at_hour_cume_dist")).otherwise(0)).alias("spring10_created_at_hour_per_created_at_hour_cume_dist_minus_created_at_hour_cume_dist"),
    )
    .orderBy(
        F.col("avg_created_at_hour_per_created_at_hour_cume_dist_minus_created_at_hour_cume_dist").desc(),
    )
)
