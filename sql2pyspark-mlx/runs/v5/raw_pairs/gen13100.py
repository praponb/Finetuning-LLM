from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_order_items
    .filter(F.col("order_id").isin([9, 12, 30]))
    .select("id", "product_id", "quantity", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
)
