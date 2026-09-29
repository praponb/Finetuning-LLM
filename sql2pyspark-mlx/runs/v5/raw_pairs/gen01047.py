from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "order_id").orderBy(F.col("paid_at").desc_nulls_last(), "id")

result = (
    df_payments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "order_id", "method", "amount", "paid_at", "status")
)
