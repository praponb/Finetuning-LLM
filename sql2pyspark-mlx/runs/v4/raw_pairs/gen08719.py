from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("method", "status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_payments
    .select(
        "*",
        F.avg("amount").over(w_recent).alias("amount_avg_7d"),
        F.ntile(4).over(w2).alias("amount_bucket"),
        F.last("amount", ignorenulls=True).over(w3).alias("last_known_amount"),
    )
)

result = (
    scored
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "amount_avg_7d",
        "amount_bucket",
    )
)
