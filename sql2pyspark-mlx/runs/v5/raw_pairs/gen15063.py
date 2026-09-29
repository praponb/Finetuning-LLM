from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units_rounded").desc_nulls_first(), "id")
revenue_by_product_id = df_store_sales.groupBy("product_id").agg(F.sum("revenue").alias("sum_store_sale_revenue"))

base = (
    df_store_sales
    .filter(F.col("store_id").isin([4, 5]))
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue",
        F.round("units", 1).alias("units_rounded"),
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
    )
)

deduped = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

joined = (
    deduped
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.units_rounded",
        "a.rn",
        F.col("b.region"),
        F.col("b.store_name"),
        F.col("b.city"),
    )
)

with_dims = (
    joined
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.price"), F.col("p.brand"))
)

remaining_keys = (
    with_dims
    .select("store_id")
    .intersect(
        df_store_sales
        .filter(F.col("units").isNotNull())
        .select("store_id")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

base_store_sales = (
    reconciled
    .filter(F.col("city") == "Bangkok")
    .select(
        "product_id",
        "units_rounded",
        "rn",
        "region",
        "city",
        "price",
        "brand",
        (F.col("price") - F.col("units_rounded")).alias("price_minus_units_rounded"),
        F.trim("city").alias("clean_city"),
    )
)

valid = (
    base_store_sales
    .filter(F.col("product_id").isin([6, 9]))
    .select(
        "product_id",
        "units_rounded",
        "rn",
        "city",
        "price",
        "brand",
        "clean_city",
        F.upper("region").alias("region_upper"),
        F.coalesce("city", F.lit("DIRECT")).alias("resolved_city"),
    )
)

result = (
    valid
    .join(revenue_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "rn",
        "city",
        "price",
        "brand",
        "clean_city",
        "resolved_city",
        F.col("sum_store_sale_revenue"),
    )
    .orderBy(F.col("rn").desc_nulls_first())
)
