from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc())

result = (
    df_payments
    .select("id", "order_id", "paid_at", "status", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "order_id", "paid_at", "status")
)
