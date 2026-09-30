from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("method", "status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_payments
    .select(
        "id",
        "order_id",
        F.ntile(3).over(w).alias("amount_bucket"),
        F.avg("amount").over(w2).alias("amount_avg_3_prior"),
        F.sum("amount").over(w3).alias("cumulative_amount"),
        F.last("amount", ignorenulls=True).over(w4).alias("last_known_amount"),
    )
)
