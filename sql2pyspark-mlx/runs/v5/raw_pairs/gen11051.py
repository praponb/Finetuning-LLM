from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("order_date").desc())

result = (
    df_orders
    .select("id", "store_id", "order_date", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "store_id", "order_date")
)
