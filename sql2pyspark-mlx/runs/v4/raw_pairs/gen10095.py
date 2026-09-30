from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity")

result = (
    df_order_items
    .filter(F.col("order_id").isin([12, 21, 24]))
    .select("id", "product_id", "quantity", F.rank().over(w).alias("rnk"))
)
