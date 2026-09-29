from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first(), "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("line_total", (F.col("quantity") + F.col("unit_price")))
)

result = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "order_id", "unit_price", "line_total")
)
