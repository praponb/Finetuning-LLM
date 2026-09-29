from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "order_id")

result = (
    df_payments
    .select(
        "id",
        "method",
        "order_id",
        "amount",
        F.avg("amount").over(w).alias("method_avg_amount"),
    )
)
