from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").desc())

result = (
    df_payments
    .select("id", "method", "paid_at", "amount", F.dense_rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 1)
    .select("id", "method", "paid_at", "amount")
    .orderBy("method", "paid_at")
)
