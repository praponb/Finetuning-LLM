from pyspark.sql import functions as F

valid = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.round((F.col("units") * 0.85), 2).alias("units_adjusted"),
        F.coalesce("units", F.lit(0)).alias("units_filled"),
    )
)

valid_store_sales = (
    valid
    .filter((F.col("sale_date") >= F.lit("2023-05-02").cast("date")) & F.col("store_id").isin([1, 4]))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "units_adjusted",
        "units_filled",
        F.round((F.col("units_adjusted") * 0.85), 2).alias("units_adjusted_scaled"),
        F.ceil("units_filled").alias("units_filled_whole"),
    )
)

kept_keys = (
    valid_store_sales
    .select("product_id")
    .intersectAll(
        df_store_sales
        .filter(F.col("revenue") <= 4000)
        .select("product_id")
    )
)

reconciled = (
    valid_store_sales
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

kept_keys_2 = (
    reconciled
    .select("product_id", "id")
    .subtract(
        df_store_sales
        .filter(F.col("sale_date") < "2023-08-09")
        .select("product_id", "id")
    )
)

kept_rows = (
    reconciled
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("product_id", "units_filled", "units_adjusted_scaled", "units_filled_whole")
    .orderBy("product_id")
)
