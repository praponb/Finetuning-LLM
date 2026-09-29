from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method").orderBy("amount", "id")

ranked = (
    df_payments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = ranked.select("id", "order_id", "amount")
