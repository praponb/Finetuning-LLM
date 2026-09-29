from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category").orderBy(F.col("stock_qty").desc())
w_hist = Window.partitionBy("supplier_id", "category").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("supplier_id", "brand").orderBy(F.col("price").desc())
w5 = Window.partitionBy("category").orderBy("launch_date", "id")
w6 = Window.orderBy("row_num", "id")
w7 = Window.partitionBy("supplier_id", "brand").orderBy(F.col("cumulative_quantity").desc(), "id")

with_latest = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock"), F.col("c.quantity"))
)

enriched = (
    with_latest
    .select(
        "*",
        F.dense_rank().over(w2).alias("stock_qty_dense_rank"),
        F.sum("quantity").over(w_hist).alias("cumulative_quantity"),
    )
)

ranked = (
    enriched
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    ranked
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("prev_stock_qty", F.lag("stock_qty", 1).over(w6))
    .withColumn("cumulative_quantity_bucket", F.ntile(3).over(w7))
)

enriched_products = (
    with_windows
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.rating"), F.col("p.city"), F.col("p.supplier_name"))
)

result = (
    enriched_products
    .groupBy("city", "supplier_id")
    .pivot("category", ["Electronics", "Fashion"])
    .agg(F.max("stock_qty").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
