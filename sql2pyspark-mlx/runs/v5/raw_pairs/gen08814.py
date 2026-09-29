from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc())

result = (
    df_order_items
    .filter(F.col("order_id").isin([3, 5, 34, 38]))
    .select("id", "order_id", "unit_price", F.rank().over(w).alias("unit_price_rank"))
)
