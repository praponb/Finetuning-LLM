from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "store_id").orderBy(F.col("units").desc(), "id")
w2 = Window.partitionBy("store_id", "product_id")
w3 = Window.partitionBy("city", "product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")

enriched = (
    df_store_sales
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.city"), F.col("p.store_name"))
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched_store_sales = (
    latest
    .withColumn("revenue_share", F.round((F.col("revenue") / F.sum("revenue").over(w2)), 4))
    .withColumn("cumulative_units", F.sum("units").over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
)

joined = (
    enriched_store_sales
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.category"), F.col("d.stock_qty"))
)

with_dims = (
    joined
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.store_name").alias("store_store_name"), F.col("s.region"))
)

with_dims_store_sales = (
    with_dims
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "inner")
    .select("e.*", F.col("s.city").alias("store_city"), F.col("s.sqft"))
)

joined_store_sales = (
    with_dims_store_sales
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.stock_qty").alias("product_stock_qty"))
)

result = (
    joined_store_sales
    .select("id", "store_city", "revenue_share", "revenue")
    .unpivot(
        ["id", "store_city"],
        ["revenue_share", "revenue"],
        "metric",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
