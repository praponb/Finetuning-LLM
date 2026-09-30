from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity_num").asc_nulls_last(), "id")

cleaned = (
    df_order_items
    .filter(F.col("order_id").isin([7, 23, 40]))
    .withColumn("quantity_num", F.col("quantity").cast("double"))
)

result = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "order_id", "product_id", "quantity", "unit_price")
)
