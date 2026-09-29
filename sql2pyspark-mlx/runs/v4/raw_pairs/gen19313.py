from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_city").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city", "category").orderBy("stock_qty", "id")
w3 = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("city", "product_id").orderBy(F.col("cumulative_stock_qty").desc_nulls_last())
w5 = Window.partitionBy("store_region").orderBy(F.col("sqft").desc_nulls_last(), "id")

with_dims = (
    df_store_sales
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.sqft"), F.col("b.region"), F.col("b.opened_date"))
)

joined = (
    with_dims
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.category"), F.col("p.stock_qty"), F.col("p.cost"))
)

with_dims_store_sales = (
    joined
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.store_name"), F.col("p.city"))
)

enriched = (
    with_dims_store_sales
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "inner")
    .select(
        "a.*",
        F.col("b.region").alias("store_region"),
        F.col("b.city").alias("store_city"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.sum("stock_qty").over(w).alias("cumulative_stock_qty"),
        F.lag("revenue", 1).over(w2).alias("prev_revenue"),
        F.avg("units").over(w3).alias("units_avg_3_prior"),
    )
)

ranked = (
    with_windows
    .withColumn("rn", F.rank().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

deduped = (
    ranked
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select(
        "id",
        "store_id",
        "sale_date",
        "region",
        "stock_qty",
        "cost",
        "store_name",
        "store_city",
        "cumulative_stock_qty",
    )
    .orderBy("store_city", "store_id")
)
