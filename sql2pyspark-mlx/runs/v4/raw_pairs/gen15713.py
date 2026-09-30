from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_order_items
    .withColumn("running_quantity", F.sum("quantity").over(w))
    .withColumn("last_known_unit_price", F.last("unit_price", ignorenulls=True).over(w2))
)

result = (
    scored
    .select("id", "order_id", "unit_price", "running_quantity", "last_known_unit_price")
)
