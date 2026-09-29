from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "channel").orderBy(F.col("amount").desc())

result = (
    df_orders
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "customer_id", "store_id", "status", "channel", "amount")
)
