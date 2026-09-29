from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_payments
    .select(
        "id",
        "order_id",
        F.avg("amount").over(w).alias("last4_avg_amount"),
        F.max("amount").over(w).alias("last4_max_amount"),
        F.sum("amount").over(w).alias("last4_sum_amount"),
    )
)
