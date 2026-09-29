from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w3 = Window.orderBy("price", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("supplier_id").orderBy("inventory_quantity", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("supplier_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("category")
w7 = Window.partitionBy("brand")
w8 = Window.partitionBy("category").orderBy("inventory_quantity_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("supplier_id").orderBy("inventory_quantity_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("category").orderBy(F.col("inventory_quantity").desc(), "id")

with_last = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock"))
)

latest_match = (
    with_last
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock").alias("inventory_last_restock"))
)

with_last_products = (
    latest_match
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock").alias("inventory_last_restock_2"),
    )
)

with_windows = (
    with_last_products
    .select(
        "*",
        F.avg("inventory_quantity").over(w3).alias("inventory_quantity_avg_5"),
        F.avg("inventory_quantity").over(w4).alias("inventory_quantity_avg_6_prior"),
        F.sum("stock_qty").over(w5).alias("running_stock_qty"),
        F.corr("cost", "price").over(w6).alias("corr_cost_price"),
        F.corr("stock_qty", "cost").over(w7).alias("corr_stock_qty_cost"),
    )
)

enriched = (
    with_windows
    .withColumn("cumulative_price", F.sum("price").over(w8))
    .withColumn("cumulative_cost", F.sum("cost").over(w9))
    .withColumn("last_known_inventory_quantity_avg_5", F.last("inventory_quantity_avg_5", ignorenulls=True).over(w10))
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w11))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    deduped
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        "inventory_last_restock",
    )
    .orderBy(F.col("id").desc())
    .limit(20)
)
