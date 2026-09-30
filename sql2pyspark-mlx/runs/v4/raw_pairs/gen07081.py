from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc())

latest = (
    df_payments
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .select("id", "order_id", "method", "amount", "paid_at", "status", "rn")
    .orderBy(F.col("amount").desc())
)
