from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("units").desc_nulls_first())
w2 = Window.partitionBy("product_id", "store_id")
w3 = Window.partitionBy("store_id")
w4 = Window.orderBy(F.col("revenue").desc_nulls_first())
w5 = Window.orderBy(F.col("units").asc_nulls_last())
w6 = Window.orderBy(F.col("store_id_min_units").desc_nulls_first())
w7 = Window.orderBy(F.col("units_share").asc_nulls_last())

remaining_keys = (
    df_store_sales
    .select("id")
    .subtract(
        df_store_sales
        .filter(F.col("store_id").isin([2, 3, 4, 5]))
        .select("id")
    )
)

kept_rows = (
    df_store_sales
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        F.rank().over(w).alias("units_rank"),
        F.round((F.col("units") / F.sum("units").over(w2)), 4).alias("units_share"),
        F.min("units").over(w3).alias("store_id_min_units"),
    )
)

metrics_store_sales = (
    metrics
    .select(
        "*",
        F.cume_dist().over(w4).alias("revenue_cume_dist"),
        F.rank().over(w5).alias("units_rank_2"),
        F.rank().over(w6).alias("store_id_min_units_rank"),
        F.rank().over(w7).alias("units_share_rank"),
    )
)

cleaned = (
    metrics_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-08-22").cast("date")) & F.col("sale_date").between("2023-04-01", "2024-04-01"))
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        "units_rank",
        "units_share",
        "revenue_cume_dist",
        "units_rank_2",
        "store_id_min_units_rank",
        "units_share_rank",
        (F.col("units_share") - F.col("store_id_min_units_rank")).alias("units_share_minus_store_id_min_units_rank"),
        F.round((F.col("units_share") * 1.15), 2).alias("units_share_scaled"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("units_share_scaled") >= 71, "LARGE").otherwise("SMALL").alias("units_share_scaled_tier"),
    )
    .agg(
        F.min("units").alias("min_units"),
        F.round(F.avg("store_id_min_units_rank"), 2).alias("avg_store_id_min_units_rank"),
        F.round(F.avg("store_id_min_units_rank"), 2).alias("avg_store_id_min_units_rank_2"),
    )
    .filter(F.col("avg_store_id_min_units_rank_2") < 1000000)
    .orderBy("avg_store_id_min_units_rank_2")
)
