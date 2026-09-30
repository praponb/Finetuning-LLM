from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("order_id") == 23)
    .select("id", "method", "paid_at", F.row_number().over(w).alias("rn"))
    .orderBy(F.col("id").desc())
)
