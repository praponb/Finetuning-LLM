from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date", "id")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("city", "region")
w4 = Window.partitionBy("city").orderBy("sqft_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city", "region").orderBy(F.col("running_sum_sqft_share").desc(), "id")

metrics = (
    df_stores
    .filter(F.col("sqft").between(5375, 14760))
    .select(
        "id",
        "city",
        "region",
        "sqft",
        F.lag("sqft", 2, 0).over(w).alias("prev_sqft"),
        F.sum("sqft").over(w2).alias("city_sum_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share"),
    )
)

cleaned = (
    metrics
    .filter((~F.col("region").eqNullSafe(F.lit("North"))) & F.col("region").isNotNull())
    .select(
        "*",
        F.when(F.col("city") == "New York", 1).otherwise(0).alias("is_new_york"),
        F.when(F.col("sqft") >= 13087, "High").when(F.col("sqft") >= 2904, "Medium").otherwise("Low").alias("sqft_level"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_filled"),
    )
)

metrics_stores = (
    cleaned
    .select(
        "*",
        F.sum("sqft_share").over(w4).alias("running_sum_sqft_share"),
        F.count("sqft_share").over(w4).alias("running_count_sqft_share"),
        F.min("sqft_share").over(w4).alias("running_min_sqft_share"),
    )
)

base = (
    metrics_stores
    .filter((~F.col("region").eqNullSafe(F.lit("Central"))) & F.col("city").isin(["Bangkok", "Singapore"]))
    .withColumn("is_new_york_filled", F.coalesce("is_new_york", F.lit(0)))
)

cleaned_stores = (
    base
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "city",
        "region",
        "sqft",
        "prev_sqft",
        "city_sum_sqft",
        "sqft_share",
        "is_new_york",
        "sqft_level",
        "region_filled",
        "running_sum_sqft_share",
        "running_count_sqft_share",
        "running_min_sqft_share",
        "is_new_york_filled",
        F.round("sqft_share", 2).alias("sqft_share_rounded"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_clean"),
    )
)

deduped = (
    cleaned_stores
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "city",
        "sqft",
        "city_sum_sqft",
        "sqft_share",
        "sqft_level",
        "region_filled",
        "is_new_york_filled",
        "sqft_share_rounded",
    )
    .orderBy(F.col("id").asc_nulls_last(), F.col("sqft_share_rounded").desc())
    .limit(10)
)
