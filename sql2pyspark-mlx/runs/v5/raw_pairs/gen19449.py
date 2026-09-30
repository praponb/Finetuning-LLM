from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("amount").desc())

result = (
    df_orders
    .select("id", "store_id", "amount", "order_date", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .orderBy(F.col("pos").desc())
)
