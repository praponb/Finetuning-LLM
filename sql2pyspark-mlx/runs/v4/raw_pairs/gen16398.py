from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("unit_price").desc())

result = (
    df_order_items
    .filter(F.col("order_id").isin([1, 5, 13]))
    .select("id", "unit_price", F.dense_rank().over(w).alias("unit_price_dense_rank"))
)
