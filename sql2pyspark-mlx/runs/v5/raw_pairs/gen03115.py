from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("element").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("element")

flattened = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("element"))

base = (
    flattened
    .filter(F.col("element").isNotNull())
    .select("*", F.hour("created_at").alias("created_at_hour"))
)

valid = (
    base
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & F.col("element").isNotNull())
    .select(
        "*",
        F.to_date("created_at").alias("created_at_date"),
        F.when(F.col("element").isin(["SPRING10", "WELCOME5"]), "Priority").otherwise("Other").alias("element_group"),
    )
)

valid_carts = (
    valid
    .filter(F.col("created_at_date").between("2023-08-01", "2025-01-01"))
    .select(
        "created_at",
        "element",
        "created_at_hour",
        "element_group",
        F.round((F.col("created_at_hour") * 1.07), 2).alias("created_at_hour_adjusted"),
        F.coalesce("element", F.lit("NONE")).alias("element_filled"),
    )
)

scored = (
    valid_carts
    .select(
        "*",
        F.count("created_at_hour").over(w).alias("created_at_hour_count_30m"),
        F.count("created_at_hour_adjusted").over(w2).alias("created_at_hour_adjusted_count_15m"),
        F.max("created_at_hour").over(w3).alias("element_max_created_at_hour"),
        F.max("created_at_hour").over(w3).alias("element_max_created_at_hour_2"),
        F.max("created_at_hour").over(w2).alias("created_at_hour_max_15m"),
    )
)

base_carts = (
    scored
    .filter((F.col("created_at") > F.to_timestamp(F.lit("2025-03-01 21:00:00"))) & (F.col("element").eqNullSafe(F.lit("WELCOME5"))))
    .withColumn("created_at_label", F.date_format("created_at", "MMM yyyy"))
    .withColumn("resolved_element", F.coalesce("element", F.lit("NONE")))
)

valid_carts_2 = (
    base_carts
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 11:00:00"))) & (F.col("created_at_hour_max_15m") <= 60))
    .select(
        "*",
        F.least("created_at_hour_adjusted", "created_at_hour").alias("least_created_at_hour_adjusted_created_at_hour"),
        F.coalesce("element", F.lit("N/A")).alias("element_clean"),
    )
)

daily = (
    valid_carts_2
    .groupBy(
        F.when(F.col("created_at_hour") >= 64, "HIGH").otherwise("LOW").alias("created_at_hour_tier"),
    )
    .agg(
        F.avg(F.when(F.col("element") == "WELCOME5", F.col("created_at_hour"))).alias("welcome5_avg_created_at_hour"),
        F.count(F.when(F.col("element") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.percentile_approx("element_max_created_at_hour_2", 0.5).alias("element_max_created_at_hour_2_p50"),
        F.min("created_at").alias("first_seen"),
        F.count(F.when(F.col("element") == "BUNDLE", 1)).alias("bundle_count"),
    )
)

result = (
    daily
    .select(
        "created_at_hour_tier",
        "welcome5_avg_created_at_hour",
        "welcome5_rows",
        "first_seen",
    )
    .orderBy("first_seen")
)
