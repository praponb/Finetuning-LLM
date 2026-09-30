from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").asc_nulls_last(), "id")
w2 = Window.partitionBy("city", "region").orderBy(F.col("opened_date_plus_months").asc_nulls_first(), "id")

base = (
    df_stores
    .filter(F.upper(F.trim("store_name")) == "OLD TOWN")
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.coalesce("region", F.lit("DIRECT")).alias("resolved_region"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

cleaned = (
    ranked
    .filter(~F.col("store_name").like("Dow%"))
    .select(
        "*",
        (F.col("sqft") * F.col("latest_rank")).alias("sqft_times_latest_rank"),
        F.add_months("opened_date", 3).alias("opened_date_plus_months"),
        F.coalesce("region", F.lit("OTHER")).alias("region_filled"),
    )
)

ranked_stores = (
    cleaned
    .withColumn("latest_rank_2", F.row_number().over(w2))
    .filter(F.col("latest_rank_2") == 1)
)

kept_keys = (
    ranked_stores
    .select("city")
    .intersect(
        df_stores
        .filter(F.col("opened_date") > "2025-10-18")
        .select("city")
    )
)

reconciled = (
    ranked_stores
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

grouped = (
    reconciled
    .groupBy(F.when(F.col("sqft") >= 14002, "HIGH").otherwise("LOW").alias("sqft_tier"))
    .agg(
        F.round(F.avg("sqft_times_latest_rank"), 2).alias("avg_sqft_times_latest_rank"),
        F.count("*").alias("num_rows"),
    )
)

result = grouped.select("sqft_tier", "avg_sqft_times_latest_rank", "num_rows")
