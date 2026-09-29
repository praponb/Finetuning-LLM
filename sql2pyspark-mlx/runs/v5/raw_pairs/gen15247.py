from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id").rowsBetween(-5, Window.currentRow)

scored = (
    df_order_items
    .withColumn("next_quantity", F.lead("quantity").over(w))
    .withColumn("prev_unit_price", F.lag("unit_price").over(w))
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("unit_price_avg_5", F.avg("unit_price").over(w2))
)

result = (
    scored
    .select("id", "product_id", "quantity", "unit_price", "next_quantity", "seq_no")
    .orderBy(F.col("product_id").desc_nulls_first())
)
