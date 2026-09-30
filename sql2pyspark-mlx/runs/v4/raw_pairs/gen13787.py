from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity")

result = (
    df_order_items
    .select("id", "product_id", "quantity", "unit_price", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .orderBy(F.col("product_id").desc())
)
