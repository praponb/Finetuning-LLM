from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc())

result = (
    df_order_items
    .filter(F.col("product_id") == 36)
    .select("id", "order_id", "quantity", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
)
