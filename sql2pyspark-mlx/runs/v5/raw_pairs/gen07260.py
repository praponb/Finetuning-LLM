from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id")
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("order_id", "status")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        F.ntile(10).over(w).alias("amount_bucket"),
        F.avg("amount").over(w2).alias("status_avg_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
    )
)
