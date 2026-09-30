from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy(F.col("last7_min_sqft").desc())
w5 = Window.partitionBy("city").orderBy("last7_max_sqft", "id").rowsBetween(-5, Window.currentRow)

cleaned = (
    df_stores
    .filter((~F.col("region").eqNullSafe(F.lit("Central"))) & F.col("region").isNotNull())
    .select(
        "*",
        F.concat("city", F.lit(" "), "store_name").alias("city_store_name"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_clean"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.discount") >= 0.3), "left_anti")
)

metrics = (
    kept
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        "region_clean",
        F.max("sqft").over(w_part).alias("last7_max_sqft"),
        F.min("sqft").over(w_part).alias("last7_min_sqft"),
        F.count("sqft").over(w_part).alias("last7_count_sqft"),
    )
)

scored = (
    metrics
    .filter(F.col("store_name").like("%Riversid%"))
    .select(
        "id",
        "store_name",
        "sqft",
        "region_clean",
        "last7_max_sqft",
        "last7_min_sqft",
        "last7_count_sqft",
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
        F.sum("last7_min_sqft").over(w3).alias("cumulative_last7_min_sqft"),
        F.rank().over(w4).alias("last7_min_sqft_rank"),
        F.avg("last7_min_sqft").over(w5).alias("last7_min_sqft_avg_5"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("last7_count_sqft") >= 16, "LARGE").otherwise("SMALL").alias("last7_count_sqft_tier"),
    )
    .agg(
        F.avg("last7_max_sqft").alias("avg_last7_max_sqft"),
        F.sum("sqft").alias("total_sqft"),
        F.stddev_samp("sqft").alias("sqft_stddev"),
    )
)
