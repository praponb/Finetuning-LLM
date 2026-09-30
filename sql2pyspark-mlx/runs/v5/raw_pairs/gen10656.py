from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount")

result = (
    df_payments
    .filter(F.col("order_id").isin([15, 16]))
    .select("id", "status", "amount", "paid_at", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "status", "amount", "paid_at")
    .orderBy(F.col("id").desc())
)
