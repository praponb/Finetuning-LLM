from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc())

result = (
    df_payments
    .filter(F.col("method") == "card")
    .select("id", "status", "amount", "method", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .select("id", "status", "amount", "method")
    .orderBy(F.col("status").desc())
)
