from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_date", "id")
w2 = Window.orderBy("cost", "id").rowsBetween(-6, Window.currentRow)
w_hist = Window.partitionBy("supplier_id", "category").orderBy("stock_qty", "id").rowsBetween(-6, -1)

cleaned = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & F.month("launch_date").isin([3, 5, 7]))
    .select(
        "*",
        F.initcap(F.trim("category")).alias("category_title"),
        F.upper(F.trim("product_name")).alias("product_name_normalized"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.avg("cost").over(w2).alias("cost_avg_6"),
        F.avg("stock_qty").over(w_hist).alias("stock_qty_avg_6_prior"),
    )
)

with_dims = (
    scored
    .alias("src")
    .join(df_suppliers.alias("d"), F.col("src.supplier_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.city"), F.col("d.rating"), F.col("d.supplier_name"))
)

result = (
    with_dims
    .select("id", "city", "rating", "price", "cost")
    .unpivot(["id", "city"], ["rating", "price", "cost"], "metric", "reading")
    .filter(F.col("reading").isNotNull())
)
