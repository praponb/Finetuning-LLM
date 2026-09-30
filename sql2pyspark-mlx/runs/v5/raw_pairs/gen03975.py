from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "store_id").orderBy("revenue_per_unit", "id")
w2 = Window.partitionBy("city").orderBy(F.col("units").desc())
w_hist = Window.partitionBy("store_id").orderBy("revenue_per_unit")
w4 = Window.partitionBy("city")

base = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-09-14")))
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
    )
)

with_dims = (
    base
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.store_id",
        "s.units",
        "s.revenue_per_unit",
        F.col("p.region"),
        F.col("p.city"),
    )
)

with_windows = (
    with_dims
    .select(
        "*",
        F.lag("units").over(w).alias("prev_units"),
        F.lag("revenue_per_unit").over(w).alias("prev_revenue_per_unit"),
        F.row_number().over(w).alias("event_seq"),
        F.cume_dist().over(w2).alias("units_cume_dist"),
    )
)

matched_keys = (
    with_windows
    .select("store_id")
    .subtract(
        df_store_sales
        .filter(F.col("sale_date") <= "2023-08-01")
        .select("store_id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "*",
        F.dense_rank().over(w_hist).alias("revenue_per_unit_dense_rank"),
        F.min("revenue_per_unit").over(w4).alias("city_min_revenue_per_unit"),
    )
)

base_store_sales = (
    scored
    .filter((~F.col("region").eqNullSafe(F.lit("North"))) & (F.col("revenue_per_unit_dense_rank") < 8))
    .select(
        "*",
        F.round((F.col("revenue_per_unit_dense_rank") * 0.9), 2).alias("revenue_per_unit_dense_rank_scaled"),
    )
)

result = (
    base_store_sales
    .groupBy(
        F.when(F.col("revenue_per_unit_dense_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("revenue_per_unit_dense_rank_tier"),
        F.when(F.col("revenue_per_unit_dense_rank_scaled") >= 77, "PREMIUM").otherwise("STANDARD").alias("revenue_per_unit_dense_rank_scaled_tier"),
    )
    .agg(
        F.percentile_approx("prev_units", [0.25, 0.5, 0.75]).alias("prev_units_quartiles"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") > 1)
)
