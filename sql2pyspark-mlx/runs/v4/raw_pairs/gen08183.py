from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("product_id", "store_id")
w_recent_2 = Window.partitionBy("store_id").orderBy(F.col("revenue_per_unit").desc(), "id")
revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.avg("revenue").alias("avg_store_sale_revenue"))

enriched = (
    df_store_sales
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.store_name"))
)

base = (
    enriched
    .filter(F.year("sale_date") >= 2025)
    .select("*", (F.col("revenue") * F.col("units")).alias("revenue_per_unit"))
)

with_dims = (
    base
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.brand"), F.col("d.price"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.sum("units").over(w_recent).alias("product_id_sum_units"),
        F.row_number().over(w_recent_2).alias("row_seq"),
    )
)

result = (
    with_windows
    .join(revenue_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "units",
        "store_name",
        "revenue_per_unit",
        "brand",
        "price",
        "row_seq",
        F.col("avg_store_sale_revenue"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
