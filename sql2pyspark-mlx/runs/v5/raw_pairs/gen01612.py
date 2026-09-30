from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("stock_qty").asc_nulls_last())
w2 = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id", "brand").orderBy("stock_qty", "id")

base = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 4]))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.when(F.col("quantity") >= 203, "High").when(F.col("quantity") >= 96, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

with_dims = (
    base
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.last_restock",
        "e.quantity_tier",
        F.col("p.stock_qty"),
        F.col("p.product_name"),
        F.col("p.brand"),
    )
)

metrics = (
    with_dims
    .withColumn("stock_qty_cume_dist", F.cume_dist().over(w))
    .withColumn("cumulative_stock_qty", F.sum("stock_qty").over(w2))
    .withColumn("stock_qty_bucket", F.ntile(5).over(w3))
)

joined = (
    metrics
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.product_id",
        "src.stock_qty",
        "src.brand",
        "src.cumulative_stock_qty",
        F.col("b.category"),
        F.col("b.price"),
        F.col("b.brand").alias("product_brand"),
    )
)

result = (
    joined
    .select("id", "product_id", "brand", "cumulative_stock_qty", "category")
    .orderBy("product_id")
)
