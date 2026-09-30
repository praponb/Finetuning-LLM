from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("supplier_id").orderBy("stock_qty", "id").rowsBetween(-2, Window.currentRow)

cleaned = (
    df_products
    .filter(F.col("cost") > 720)
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.round("price", 1).alias("price_rounded"),
    )
)

joined = (
    cleaned
    .alias("src")
    .join(df_suppliers.alias("b"), F.col("src.supplier_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.supplier_name"))
)

eligible = (
    joined
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.sale_date") >= F.col("s.launch_date")), "left_anti")
)

eligible_products = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.sale_date") <= F.col("s.launch_date")), "left_anti")
)

joined_products = (
    eligible_products
    .alias("src")
    .join(df_suppliers.alias("d"), F.col("src.supplier_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.country"))
)

result = (
    joined_products
    .filter(F.month("launch_date").isin([4, 6, 9]))
    .select(
        "*",
        F.max("cost").over(w_hist).alias("last3_max_cost"),
        F.avg("cost").over(w_hist).alias("last3_avg_cost"),
        F.sum("cost").over(w_hist).alias("last3_sum_cost"),
    )
)
