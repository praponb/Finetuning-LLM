from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at_hour_filled").desc())

exploded = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("value"))

filtered = (
    exploded
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-02"))) & (F.to_date("created_at") < "2023-06-22"))
    .select(
        "*",
        F.hour("created_at").alias("created_at_hour"),
        F.lower("value").alias("value_lower"),
        F.coalesce("value", F.lit("OTHER")).alias("value_filled"),
    )
)

valid = (
    filtered
    .filter((F.col("created_at") > F.lit("2025-03-02 01:00:00").cast("timestamp")) & (F.col("value").eqNullSafe(F.lit("BUNDLE"))))
    .select(
        "*",
        F.when(F.col("created_at_hour") >= 90, "High").when(F.col("created_at_hour") >= 19, "Medium").otherwise("Low").alias("created_at_hour_band"),
        F.coalesce("created_at_hour", F.lit(0)).alias("created_at_hour_filled"),
    )
)

base = (
    valid
    .filter((F.col("created_at") > F.lit("2025-03-01 13:00:00").cast("timestamp")) & F.col("value").isNotNull())
    .select(
        "id",
        "value_filled",
        "created_at_hour_band",
        "created_at_hour_filled",
        F.to_date("created_at").alias("created_at_date"),
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
    )
)

scored = (
    base
    .filter(F.year("created_at_date") >= 2024)
    .select("*", F.cume_dist().over(w).alias("created_at_hour_filled_cume_dist"))
)

cleaned = (
    scored
    .filter(F.year("created_at_date") >= 2024)
    .select(
        "*",
        F.ceil("created_at_hour_filled_cume_dist").alias("created_at_hour_filled_cume_dist_whole"),
    )
)

result = (
    cleaned
    .select("id", "value_filled", "created_at_hour_filled_cume_dist")
    .orderBy(F.col("created_at_hour_filled_cume_dist").desc(), "id")
)
