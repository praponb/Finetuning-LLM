from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id")
w2 = Window.partitionBy("product_id").orderBy(F.col("units").desc())
w_grp = Window.partitionBy("store_id").orderBy(F.col("units_dense_rank").asc_nulls_last())
w4 = Window.partitionBy("store_id").orderBy(F.col("units_dense_rank").desc_nulls_first())
w5 = Window.partitionBy("store_id")
w6 = Window.partitionBy("product_id").orderBy(F.col("store_id_max_units_share").desc())

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-09-14"))) & F.col("revenue").between(1100, 2600))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)

remaining_keys = (
    cleaned
    .select("product_id", "store_id")
    .intersect(
        df_store_sales
        .filter(F.col("product_id").isin([5, 8, 9]))
        .select("product_id", "store_id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .filter(F.col("units").isNotNull())
    .select(
        "store_id",
        "product_id",
        F.round((F.col("units") / F.sum("units").over(w)), 4).alias("units_share"),
        F.dense_rank().over(w2).alias("units_dense_rank"),
    )
)

with_windows = (
    scored
    .select(
        "product_id",
        "units_share",
        F.rank().over(w_grp).alias("units_dense_rank_rank"),
        F.percent_rank().over(w4).alias("units_dense_rank_percent_rank"),
        F.max("units_share").over(w5).alias("store_id_max_units_share"),
    )
)

with_windows_store_sales = (
    with_windows
    .select(
        "product_id",
        "units_share",
        "units_dense_rank_rank",
        "units_dense_rank_percent_rank",
        "store_id_max_units_share",
        F.dense_rank().over(w6).alias("store_id_max_units_share_dense_rank"),
    )
)

result = (
    with_windows_store_sales
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .rollup(F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("e.store_id_max_units_share_dense_rank")).alias("min_store_id_max_units_share_dense_rank"),
    )
    .orderBy(F.col("min_store_id_max_units_share_dense_rank").asc_nulls_first())
)
