from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id")

result = (
    df_payments
    .select("id", "order_id", "amount", "paid_at", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
)
