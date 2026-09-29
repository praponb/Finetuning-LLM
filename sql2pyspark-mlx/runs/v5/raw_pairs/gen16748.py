from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("paid_at").desc())

result = (
    df_payments
    .select("id", "paid_at", F.dense_rank().over(w).alias("paid_at_dense_rank"))
)
