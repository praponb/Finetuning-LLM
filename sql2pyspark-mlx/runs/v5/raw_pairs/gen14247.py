from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc())

result = (
    df_order_items
    .filter(F.col("order_id") == 13)
    .select("id", "product_id", "unit_price", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 2)
)
