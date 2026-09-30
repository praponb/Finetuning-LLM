from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("price").desc())
w2 = Window.partitionBy("category").orderBy(F.col("stock_qty").desc(), "id")
w3 = Window.partitionBy("city").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("category", "city")
w5 = Window.orderBy("cumulative_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("country").orderBy("price_dense_rank", "id")

kept = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") < F.col("s.launch_date")), "left_anti")
)

joined = (
    kept
    .alias("src")
    .join(df_suppliers.alias("b"), F.col("src.supplier_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.city"), F.col("b.country"), F.col("b.rating"))
)

with_windows = (
    joined
    .select(
        "*",
        F.dense_rank().over(w).alias("price_dense_rank"),
        F.row_number().over(w2).alias("event_seq"),
        F.sum("cost").over(w3).alias("cumulative_cost"),
    )
)

enriched = (
    with_windows
    .filter(F.col("cumulative_cost") < 1569)
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "tags",
        "country",
        "cumulative_cost",
        F.min("price_dense_rank").over(w4).alias("category_min_price_dense_rank"),
        F.sum("rating").over(w5).alias("cumulative_rating"),
        F.ntile(10).over(w_running).alias("price_dense_rank_bucket"),
    )
)

result = (
    enriched
    .groupBy("brand", "category")
    .pivot("country", ["SG", "DE", "TH"])
    .agg(F.max("category_min_price_dense_rank"))
    .select(
        "brand",
        "category",
        F.col("SG").alias("sg"),
        F.col("DE").alias("de"),
        F.col("TH").alias("th"),
    )
)
