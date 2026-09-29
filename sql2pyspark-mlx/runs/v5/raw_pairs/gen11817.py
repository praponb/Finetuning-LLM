from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at")

result = (
    df_payments
    .select("id", "method", "paid_at", "amount", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 2)
    .select("id", "method", "paid_at", "amount")
    .orderBy(F.col("amount").desc())
)
