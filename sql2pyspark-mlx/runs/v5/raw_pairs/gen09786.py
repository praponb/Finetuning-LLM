from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").desc())

deduped = (
    df_payments
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "order_id", "method", "amount", "paid_at", "status")
)

result = (
    deduped
    .select("id", "amount")
    .orderBy("amount", F.col("id").desc())
)
