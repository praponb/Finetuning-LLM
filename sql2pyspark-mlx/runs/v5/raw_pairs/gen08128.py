from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").desc(), "id")

result = (
    df_payments
    .filter(F.col("method") != "bank_transfer")
    .select("id", "method", "paid_at", "amount", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 1)
    .orderBy("paid_at")
)
