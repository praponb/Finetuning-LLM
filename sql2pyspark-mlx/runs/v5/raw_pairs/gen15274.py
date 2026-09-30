from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-2, -1)

valid = (
    df_stores
    .filter(F.col("region").isNotNull() & (~F.col("region").eqNullSafe(F.lit("West"))))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.round((F.col("sqft") * 0.85), 2).alias("sqft_scaled"),
        F.trim("region").alias("clean_region"),
        F.coalesce("region", F.lit("N/A")).alias("region_clean"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.min("sqft").over(w).alias("running_min_sqft"),
        F.max("sqft").over(w).alias("running_max_sqft"),
        F.avg("sqft").over(w).alias("running_avg_sqft"),
        F.avg("sqft").over(w2).alias("sqft_avg_2_prior"),
    )
)

remaining_keys = (
    metrics
    .select("city")
    .intersectAll(
        df_stores
        .filter(F.col("region") == "East")
        .select("city")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "sqft_scaled",
        "clean_region",
        "region_clean",
        "running_min_sqft",
        "running_max_sqft",
        "running_avg_sqft",
        "sqft_avg_2_prior",
        (F.col("sqft_avg_2_prior") * F.col("running_max_sqft")).alias("sqft_avg_2_prior_times_running_max_sqft"),
        F.coalesce("region", F.lit("OTHER")).alias("region_filled"),
    )
)

result = (
    base
    .select(
        "id",
        "sqft_scaled",
        "region_clean",
        "running_min_sqft",
        "running_max_sqft",
        "running_avg_sqft",
        "sqft_avg_2_prior",
        "sqft_avg_2_prior_times_running_max_sqft",
        "region_filled",
    )
    .orderBy("sqft_avg_2_prior_times_running_max_sqft")
)
