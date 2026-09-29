from pyspark.sql import functions as F

filtered = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-08-14"))) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.round((F.col("sqft") * 1.15), 2).alias("sqft_adjusted"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_filled"),
    )
)

remaining_keys = (
    filtered
    .select("city", "id")
    .subtract(
        df_stores
        .filter(F.col("region").isin(["North", "West", "Central"]))
        .select("city", "id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged = (
    verified
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") >= F.col("s.opened_date")), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        F.date_trunc("YEAR", "opened_date").alias("year_start"),
        F.when(F.col("sqft_adjusted") >= 65, "LARGE").otherwise("SMALL").alias("sqft_adjusted_tier"),
    )
    .agg(
        F.percentile_approx("sqft_adjusted", [0.25, 0.5, 0.75]).alias("sqft_adjusted_quartiles"),
        F.count("*").alias("row_count"),
    )
)
