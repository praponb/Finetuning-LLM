from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category").orderBy(F.col("stock_qty").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

cleaned = (
    df_products
    .filter(F.col("brand").isNotNull() & (F.col("price") < 430))
    .withColumn("tags_text", F.array_join("tags", ", "))
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units"), F.col("c.sale_date"))
)

deduped = (
    latest_match
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_latest = (
    deduped
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.sale_date")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("inventory_quantity"))
)

eligible = (
    with_latest
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.order_id").isin([10, 17, 26, 39])), "left_anti")
)

result = (
    eligible
    .select("id", "brand", "price", "cost")
    .unpivot(["id", "brand"], ["price", "cost"], "metric_name", "value")
    .filter(F.col("value").isNotNull())
)
