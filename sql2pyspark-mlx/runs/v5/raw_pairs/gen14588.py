from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("line_total", "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("line_total", F.round((F.col("quantity") / F.col("unit_price")), 2))
    .withColumn("unit_price_level", F.when(F.col("unit_price") >= 300, "High").when(F.col("unit_price") >= 110, "Medium").otherwise("Low"))
)

result = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "quantity", "unit_price_level")
)
