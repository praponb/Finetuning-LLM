from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("units").asc_nulls_last())

valid = (
    df_store_sales
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-09-24"))) & F.col("store_id").isin([1, 4]))
    .withColumn("least_units_revenue", F.least("units", "revenue"))
    .withColumn("units_rounded", F.round("units", 2))
)

kept_keys = (
    valid
    .select("store_id")
    .exceptAll(
        df_store_sales
        .filter(F.col("product_id").isin([1, 4]))
        .select("store_id")
    )
)

verified = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "id",
        "store_id",
        "revenue",
        "least_units_revenue",
        "units_rounded",
        F.rank().over(w).alias("units_rank"),
    )
)

filtered = (
    metrics
    .filter(F.col("units_rounded") > 37)
    .select("*", F.coalesce("units_rounded", F.lit(0)).alias("units_rounded_filled"))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("units_rounded_filled") >= 67, "LARGE").otherwise("SMALL").alias("units_rounded_filled_tier"),
        F.when(F.col("units_rounded") >= 61, "LARGE").otherwise("SMALL").alias("units_rounded_tier"),
    )
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.percentile_approx("units_rank", [0.25, 0.5, 0.75]).alias("units_rank_quartiles"),
        F.percentile_approx("units_rank", 0.9).alias("units_rank_p90"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("revenue_stddev").desc())
)
