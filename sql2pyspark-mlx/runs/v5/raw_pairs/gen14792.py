from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("order_id", "status").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .select(
        "id",
        "paid_at",
        F.sum("amount").over(w).alias("running_amount"),
        F.sum("amount").over(w2).alias("running_amount_2"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
        F.ntile(3).over(w4).alias("amount_bucket"),
    )
)
