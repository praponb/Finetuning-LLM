from pyspark.sql import functions as F

units_by_store_id = df_store_sales.groupBy("store_id").agg(F.sum("units").alias("sum_store_sale_units"))

remaining_keys = (
    df_store_sales
    .select("store_id", "product_id")
    .subtract(
        df_store_sales
        .filter(F.col("units") <= 19)
        .select("store_id", "product_id")
    )
)

verified = (
    df_store_sales
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "inner")
    .select("src.*", F.col("s.opened_date"), F.col("s.sqft"), F.col("s.region"))
)

remaining_keys_2 = (
    joined
    .select("product_id", "store_id")
    .subtract(
        df_store_sales
        .filter(F.col("units") > 22)
        .select("product_id", "store_id")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .select(
        "src.store_id",
        "src.product_id",
        "src.sale_date",
        "src.units",
        "src.sqft",
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("s.region").alias("store_region"),
    )
)

result = (
    with_dims
    .join(units_by_store_id, on="store_id", how="left")
    .select(
        "store_id",
        "sale_date",
        "units",
        "store_name",
        "city",
        "store_region",
        F.when(F.col("store_region").rlike("^(South|West)$"), "GROUP_B").otherwise("CORE").alias("store_region_zone"),
        F.col("sum_store_sale_units"),
    )
    .orderBy(F.col("city").desc_nulls_first(), "sale_date")
)
