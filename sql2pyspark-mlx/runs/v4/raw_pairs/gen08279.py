from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount", "id")

result = (
    df_payments
    .select("id", "method", "amount", "order_id", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .select("id", "method", "amount", "order_id")
    .orderBy(F.col("order_id").desc())
)
