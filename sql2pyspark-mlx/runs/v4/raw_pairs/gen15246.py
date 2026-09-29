from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())

result = (
    df_order_items
    .filter(F.col("quantity") > 5)
    .select("id", "product_id", "quantity", F.dense_rank().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
    .select("id", "product_id", "quantity")
)
