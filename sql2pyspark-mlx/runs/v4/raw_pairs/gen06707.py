from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc_nulls_first(), "id")

ranked = (
    df_order_items
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

base = (
    ranked
    .filter(F.col("product_id").isNotNull())
    .select("*", (F.col("quantity") + F.col("unit_price")).alias("line_total"))
)

with_dims = (
    base
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.stock_qty"))
)

result = (
    with_dims
    .select("id", "unit_price", "latest_rank", "line_total", "stock_qty")
    .orderBy("line_total")
)
