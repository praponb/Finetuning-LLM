from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("store_id")
w3 = Window.orderBy(F.col("revenue_share_2").desc_nulls_last())

scored = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.round((F.col("revenue") / F.sum("revenue").over(w)), 4).alias("revenue_share"),
        F.round((F.col("revenue") / F.sum("revenue").over(w2)), 4).alias("revenue_share_2"),
    )
)

kept_keys = (
    scored
    .select("product_id", "id")
    .subtract(
        df_store_sales
        .filter(F.col("product_id").isin([4, 6, 8]))
        .select("product_id", "id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.opened_date"), F.col("d.city"), F.col("d.region"))
)

scored_store_sales = (
    with_dims
    .select(
        "*",
        F.min("revenue_share_2").over(w).alias("product_id_min_revenue_share_2"),
        F.cume_dist().over(w3).alias("revenue_share_2_cume_dist"),
        F.round((F.col("units") / F.sum("units").over(w)), 4).alias("units_share"),
    )
)

filtered = (
    scored_store_sales
    .filter(F.col("units_share") < 0.41)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue_share_2",
        "opened_date",
        "product_id_min_revenue_share_2",
        "revenue_share_2_cume_dist",
        "units_share",
        F.upper("city").alias("city_upper"),
        F.coalesce("city", F.lit("N/A")).alias("city_clean"),
    )
)

result = (
    filtered
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue_share_2",
        "opened_date",
        "revenue_share_2_cume_dist",
        "units_share",
        "city_upper",
    )
    .orderBy(F.col("product_id").desc_nulls_last())
)
