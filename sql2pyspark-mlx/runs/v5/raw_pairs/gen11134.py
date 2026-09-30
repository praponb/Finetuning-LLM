from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc())

result = (
    df_payments
    .select("id", "order_id", "paid_at", F.dense_rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 2)
    .select("id", "order_id", "paid_at")
    .orderBy(F.col("id").desc())
)
