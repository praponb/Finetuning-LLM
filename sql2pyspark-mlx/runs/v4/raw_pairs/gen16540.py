from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_first())

deduped = (
    df_payments
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select("id", "order_id", "paid_at", "latest_rank")
    .orderBy(F.col("latest_rank").desc())
)
