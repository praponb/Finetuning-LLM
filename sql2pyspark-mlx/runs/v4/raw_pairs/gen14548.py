from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price", "id")
w2 = Window.partitionBy("brand", "category").orderBy(F.col("latest_rank").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("category", "brand").orderBy(F.col("latest_rank").desc(), "id")
w6 = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_products
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

latest_products = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

with_latest = (
    latest_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("inventory_quantity"))
)

scored = (
    with_latest
    .withColumn("cumulative_cost", F.sum("cost").over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("last_known_stock_qty", F.last("stock_qty", ignorenulls=True).over(w6))
)

eligible = (
    scored
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.unit_price").between(260, 280)), "left_anti")
)

result = (
    eligible
    .groupBy("supplier_id")
    .agg(
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_rows"),
        F.size(F.collect_set("category")).alias("num_categories"),
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_rows"),
    )
    .filter(F.col("num_categories") < 1)
    .orderBy(F.col("num_categories").desc_nulls_last())
)
