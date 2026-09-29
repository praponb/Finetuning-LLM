from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id")

cleaned = (
    df_order_items
    .filter(F.col("quantity").between(4, 8))
    .withColumn("unit_price_filled", F.coalesce("unit_price", F.lit(0)))
)

result = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "product_id", "unit_price", "unit_price_filled")
)
