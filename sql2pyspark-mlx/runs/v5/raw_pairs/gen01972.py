from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("amount").desc())

result = (
    df_orders
    .filter(F.col("channel").isin(["store", "web"]))
    .select("id", "store_id", "amount", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 2)
    .select("id", "store_id", "amount")
)
