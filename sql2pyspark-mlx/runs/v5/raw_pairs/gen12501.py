from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method")

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "status",
        "method",
        "amount",
        F.sum("amount").over(w).alias("status_sum_amount"),
    )
)
