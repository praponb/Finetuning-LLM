from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id")
w2 = Window.partitionBy("order_id")

result = (
    df_order_items
    .withColumn("prev_unit_price", F.lag("unit_price").over(w))
    .withColumn("next_unit_price", F.lead("unit_price").over(w))
    .withColumn("prev_quantity", F.lag("quantity").over(w))
    .withColumn("next_quantity", F.lead("quantity").over(w))
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("corr_unit_price_quantity", F.corr("unit_price", "quantity").over(w2))
)
