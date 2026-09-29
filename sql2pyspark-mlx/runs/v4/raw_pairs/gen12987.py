from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("greatest_quantity_unit_price", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("unit_price").between(220, 340))
    .withColumn("greatest_quantity_unit_price", F.greatest("quantity", "unit_price"))
)

result = (
    cleaned
    .withColumn("unit_price_avg_6_prior", F.avg("unit_price").over(w))
    .withColumn("quantity_bucket", F.ntile(5).over(w2))
)
