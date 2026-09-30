from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("amount").desc())

result = (
    df_orders
    .select("id", "channel", "amount", F.dense_rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .orderBy(F.col("amount").desc())
)
