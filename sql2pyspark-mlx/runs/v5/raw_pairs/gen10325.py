from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .filter(F.col("product_id") == 25)
    .withColumn("prev_unit_price", F.lag("unit_price").over(w))
    .withColumn("prev_quantity", F.lag("quantity").over(w))
    .withColumn("next_quantity", F.lead("quantity").over(w))
    .withColumn("row_seq", F.row_number().over(w))
)
