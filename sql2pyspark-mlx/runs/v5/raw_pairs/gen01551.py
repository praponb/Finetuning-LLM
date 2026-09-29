from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status", "method")

result = (
    df_payments
    .filter(F.col("order_id").isin([1, 5, 12]))
    .select(
        "id",
        "status",
        "paid_at",
        "method",
        F.row_number().over(w).alias("paid_at_row"),
        F.count("*").over(w2).alias("status_count"),
    )
)
