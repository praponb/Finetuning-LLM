from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id")
w2 = Window.partitionBy("product_id")
discount_by_store_id = df_orders.groupBy("store_id").agg(F.min("discount").alias("min_order_discount"))

remaining_keys = (
    df_store_sales
    .select("product_id", "id")
    .subtract(
        df_store_sales
        .filter(F.col("units") == 31)
        .select("product_id", "id")
    )
)

reconciled = (
    df_store_sales
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "store_id",
        "product_id",
        "units",
        "revenue",
        F.min("units").over(w).alias("store_id_min_units"),
        F.corr("units", "revenue").over(w2).alias("corr_units_revenue"),
        F.round((F.col("units") / F.sum("units").over(w)), 4).alias("units_share"),
    )
)

with_dims = (
    metrics
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .select(
        "s.store_id",
        "s.product_id",
        "s.units",
        "s.units_share",
        F.col("p.stock_qty"),
    )
)

filtered = (
    with_dims
    .filter(F.col("units").between(35, 38))
    .withColumn("stock_qty_filled", F.coalesce("stock_qty", F.lit(0)))
    .withColumn("units_whole", F.floor("units"))
)

enriched = (
    filtered
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "inner")
    .select("e.*", F.col("s.store_name"), F.col("s.city"), F.col("s.sqft"))
)

cleaned = (
    enriched
    .filter(F.col("units_whole").between(63, 71))
    .select(
        "store_id",
        "units_share",
        "stock_qty",
        "units_whole",
        "store_name",
        "sqft",
        F.when(F.col("units_share") >= 0.68, "High").when(F.col("units_share") >= 0.36, "Medium").otherwise("Low").alias("units_share_band"),
    )
)

enriched_store_sales = (
    cleaned
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .filter(F.lower(F.col("d.store_name")).like("%own%"))
    .select(
        "a.store_id",
        "a.units_whole",
        "a.sqft",
        F.col("d.sqft").alias("store_sqft"),
        F.col("d.city"),
    )
)

wide = (
    enriched_store_sales
    .groupBy("store_id")
    .pivot("city", ["Singapore", "Berlin"])
    .agg(F.sum("store_sqft").alias("amount"), F.count(F.lit(1)).alias("n"))
)

result = (
    wide
    .join(discount_by_store_id, on="store_id", how="left")
    .select("store_id", "Berlin_amount", "Berlin_n", F.col("min_order_discount"))
    .orderBy(F.col("Berlin_amount").desc_nulls_first())
)
