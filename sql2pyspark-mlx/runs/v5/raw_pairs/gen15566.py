from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("units", "id")
w2 = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-08-27").cast("date")) & (F.col("revenue") <= 1500))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.col("revenue").cast("int").alias("revenue_int"),
    )
)

with_windows = (
    base
    .filter(F.col("product_id").isin([2, 5, 9]))
    .select(
        "id",
        "store_id",
        F.ntile(4).over(w).alias("units_bucket"),
        F.sum("revenue_int").over(w2).alias("running_revenue_int"),
    )
)

enriched = (
    with_windows
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "inner")
    .select("e.id", "e.store_id", F.col("s.city"))
)

with_dims = (
    enriched
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.sqft"), F.col("d.city").alias("store_city"))
)

cleaned = (
    with_dims
    .filter(F.col("store_id").isin([1, 2, 5, 6]))
    .select(
        "*",
        F.when(F.col("sqft") >= 12241, "High").when(F.col("sqft") >= 8199, "Medium").otherwise("Low").alias("sqft_level"),
    )
)

result = (
    cleaned
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .cube(
        F.col("d.city").alias("store_city"),
        F.col("d.region").alias("region"),
        F.col("a.city").alias("store_sale_city"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("a.sqft")), 2).alias("total_sqft"),
    )
)
