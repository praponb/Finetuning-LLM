from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .filter(F.col("amount").between(770, 1260))
    .select("id", "order_id", "amount", "method", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "order_id", "amount", "method")
    .orderBy(F.col("id").desc())
)
