from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w3 = Window.partitionBy("product_id")

result = (
    df_order_items
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("quantity_share", F.round((F.col("quantity") / F.sum("quantity").over(w3)), 4))
)
