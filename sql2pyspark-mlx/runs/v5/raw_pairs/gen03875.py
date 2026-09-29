from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc())

result = (
    df_payments
    .select("id", "amount", F.dense_rank().over(w).alias("amount_dense_rank"))
    .orderBy("amount_dense_rank")
)
