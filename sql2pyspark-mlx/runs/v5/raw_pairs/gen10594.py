from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "order_id").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_payments
    .select(
        "*",
        F.max("amount").over(w).alias("last7_max_amount"),
        F.min("amount").over(w).alias("last7_min_amount"),
        F.avg("amount").over(w).alias("last7_avg_amount"),
    )
)
