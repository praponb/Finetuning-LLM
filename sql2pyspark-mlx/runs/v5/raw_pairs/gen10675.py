from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc())

result = (
    df_order_items
    .select("id", "order_id", "unit_price", "product_id", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "unit_price", "product_id")
)
