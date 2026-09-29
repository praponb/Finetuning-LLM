from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount").desc())

result = (
    df_orders
    .select("id", "channel", "discount", "store_id", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 5)
    .select("id", "channel", "discount", "store_id")
    .orderBy(F.col("id").desc())
)
