from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("order_id").isNull())
    .select("id", "status", "paid_at", "method", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 2)
    .select("id", "status", "paid_at", "method")
)
