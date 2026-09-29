from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("order_date")

result = (
    df_orders
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "store_id", "order_date")
)
