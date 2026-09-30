from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("paid_at") >= "2024-06-01")
    .select("id", "paid_at", "amount", F.lag("amount", 1).over(w).alias("prev_amount"))
)
