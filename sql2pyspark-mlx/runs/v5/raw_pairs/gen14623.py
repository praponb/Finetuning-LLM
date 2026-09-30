from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")

flattened = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "created_at",
        F.col("line.product_id"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

base = (
    flattened
    .filter((F.col("created_at") >= F.lit("2025-03-02").cast("date")) & F.col("promo").isNotNull())
    .select(
        "*",
        F.hour("created_at").alias("created_at_hour"),
        F.coalesce("promo", F.lit("NONE")).alias("promo_filled"),
    )
)

valid = (
    base
    .filter((F.col("created_at") > F.lit("2025-03-01 13:00:00").cast("timestamp")) & F.col("promo").isNotNull())
    .select(
        "id",
        "created_at",
        "price",
        "promo",
        "created_at_hour",
        "promo_filled",
        (F.col("created_at_hour") * F.col("product_id")).alias("created_at_hour_times_product_id"),
        F.coalesce("promo", F.lit("OTHER")).alias("promo_filled_2"),
    )
)

scored = (
    valid
    .select(
        "created_at",
        "price",
        "promo",
        "created_at_hour",
        "promo_filled",
        "created_at_hour_times_product_id",
        F.max("created_at_hour_times_product_id").over(w).alias("promo_max_created_at_hour_times_product_id"),
        F.stddev_samp("created_at_hour").over(w).alias("promo_stddev_samp_created_at_hour"),
    )
)

cleaned = (
    scored
    .filter((F.col("promo").eqNullSafe(F.lit("BUNDLE"))) & F.col("promo").isNotNull())
    .withColumn("promo_stddev_samp_created_at_hour_level", F.when(F.col("promo_stddev_samp_created_at_hour") >= 77, "High").when(F.col("promo_stddev_samp_created_at_hour") >= 36, "Medium").otherwise("Low"))
    .withColumn("resolved_promo", F.coalesce("promo", F.lit("DIRECT")))
)

result = (
    cleaned
    .select(
        "created_at",
        "created_at_hour_times_product_id",
        "promo_max_created_at_hour_times_product_id",
        "promo_stddev_samp_created_at_hour_level",
    )
)
