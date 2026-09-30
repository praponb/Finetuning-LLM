from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("amount", "id")
w3 = Window.partitionBy("method")

with_windows = (
    df_payments
    .select(
        "id",
        "method",
        "status",
        F.last("method", ignorenulls=True).over(w).alias("last_known_method"),
        F.lag("amount", 1, 0).over(w2).alias("prev_amount"),
        F.avg("amount").over(w3).alias("method_avg_amount"),
    )
)

result = (
    with_windows
    .select("id", "method", "status", "last_known_method", "method_avg_amount")
    .orderBy(F.col("status").desc_nulls_last())
)
