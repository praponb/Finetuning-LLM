from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("category").orderBy(F.col("quantity_filled").desc())
w_recent = Window.partitionBy("category").orderBy("stock_qty", "id")
w_grp = Window.partitionBy("store_id")
w5 = Window.partitionBy("store_id", "category")

base = (
    df_inventory
    .filter(F.col("product_id").isin([1, 5, 7, 9]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

with_dims = (
    base
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.stock_qty"), F.col("p.brand"), F.col("p.category"))
)

enriched = (
    with_dims
    .select(
        "*",
        F.avg("stock_qty").over(w).alias("stock_qty_avg_5"),
        F.percent_rank().over(w2).alias("quantity_filled_percent_rank"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.max("quantity_filled").over(w_grp).alias("store_id_max_quantity_filled"),
        F.count("quantity").over(w5).alias("store_id_count_quantity"),
    )
)

result = (
    enriched
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "stock_qty",
        "brand",
        "category",
        "store_id_count_quantity",
    )
    .orderBy("category", F.col("brand").desc())
)
