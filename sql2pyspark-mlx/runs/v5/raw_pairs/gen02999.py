from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").desc())

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select("id", "method", "paid_at", F.dense_rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
)
