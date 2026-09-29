from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("brand").orderBy("price", "id")
w3 = Window.orderBy("price", "id")
w4 = Window.partitionBy("category").orderBy("launch_date_plus_20d", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("brand", "category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select("*", F.date_add("launch_date", 20).alias("launch_date_plus_20d"))
)

latest_match = (
    filtered
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("inventory_quantity"))
)

enriched = (
    latest_match
    .withColumn("price_bucket", F.ntile(10).over(w2))
    .withColumn("next_cost", F.lead("cost", 1).over(w3))
    .withColumn("stock_qty_avg_2", F.avg("stock_qty").over(w4))
    .withColumn("cumulative_stock_qty", F.sum("stock_qty").over(w5))
)

with_dims = (
    enriched
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.country"))
)

result = (
    with_dims
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "launch_date_plus_20d",
        "inventory_quantity",
    )
    .orderBy(F.col("supplier_id").desc())
)
