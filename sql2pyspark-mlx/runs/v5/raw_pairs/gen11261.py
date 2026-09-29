from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("sqft_rounded", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-5, Window.currentRow)
w_recent = Window.partitionBy("region", "city").orderBy("is_singapore", "id")

base = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-04-30"))) & (~F.col("region").eqNullSafe(F.lit("Central"))))
    .select(
        "*",
        F.round("sqft", 2).alias("sqft_rounded"),
        F.when(F.col("city") == "Singapore", 1).otherwise(0).alias("is_singapore"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.last("region", ignorenulls=True).over(w_hist).alias("last_known_region"),
        F.first("is_singapore", ignorenulls=True).over(w2).alias("first_known_is_singapore"),
        F.avg("sqft").over(w3).alias("sqft_avg_3_prior"),
    )
)

with_windows_stores = (
    with_windows
    .filter(F.col("first_known_is_singapore") > 57)
    .select(
        "*",
        F.avg("is_singapore").over(w4).alias("is_singapore_avg_5"),
        F.lead("sqft_avg_3_prior", 2).over(w_recent).alias("next_sqft_avg_3_prior"),
    )
)

matched_keys = (
    with_windows_stores
    .select("city")
    .exceptAll(
        df_stores
        .filter(F.col("city") == "Berlin")
        .select("city")
    )
)

kept_rows = (
    with_windows_stores
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") <= F.col("s.opened_date")), "left_semi")
)

result = (
    kept
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "sqft_rounded",
        "first_known_is_singapore",
        "sqft_avg_3_prior",
    )
    .orderBy(F.col("city").desc_nulls_last(), "id")
)
