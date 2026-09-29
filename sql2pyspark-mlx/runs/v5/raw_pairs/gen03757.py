from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id")
w_grp = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp_2 = Window.partitionBy("city", "region").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region")

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull())
    .withColumn("store_name_prefix", F.substring("store_name", 1, 3))
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

kept = (
    deduped
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.units").isNotNull()), "left_semi")
)

scored = (
    kept
    .withColumn("running_min_sqft", F.min("sqft").over(w_grp))
    .withColumn("running_avg_sqft", F.avg("sqft").over(w_grp))
    .withColumn("running_count_sqft", F.count("sqft").over(w_grp))
    .withColumn("cumulative_sqft", F.sum("sqft").over(w_grp_2))
)

remaining_keys = (
    scored
    .select("region")
    .intersectAll(
        df_stores
        .filter(F.col("opened_date").between("2024-08-07", "2025-10-01"))
        .select("region")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

kept_keys = (
    reconciled
    .select("city")
    .subtract(
        df_stores
        .filter(F.col("sqft") >= 8910)
        .select("city")
    )
)

verified = (
    reconciled
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "id",
        "running_count_sqft",
        F.min("running_avg_sqft").over(w4).alias("region_min_running_avg_sqft"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("running_count_sqft") >= 16, "LARGE").otherwise("SMALL").alias("running_count_sqft_tier"),
        F.when(F.col("region_min_running_avg_sqft") >= 13329, "PREMIUM").otherwise("STANDARD").alias("region_min_running_avg_sqft_tier"),
    )
    .agg(
        F.percentile_approx("region_min_running_avg_sqft", 0.95).alias("region_min_running_avg_sqft_p95"),
        F.avg("running_count_sqft").alias("avg_running_count_sqft"),
        F.percentile_approx("region_min_running_avg_sqft", 0.9).alias("region_min_running_avg_sqft_p90"),
        F.percentile_approx("running_count_sqft", 0.9).alias("running_count_sqft_p90"),
    )
)
