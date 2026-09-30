from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id")

result = (
    df_payments
    .filter(F.col("paid_at") < "2024-08-01")
    .select("id", "order_id", "amount", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 2)
    .select("id", "order_id", "amount")
)
