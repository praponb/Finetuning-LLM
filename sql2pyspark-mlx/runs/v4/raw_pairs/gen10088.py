from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w_ordered = Window.partitionBy("order_id").orderBy("paid_at", "id")
w3 = Window.orderBy(F.col("amount").asc_nulls_last())
w4 = Window.orderBy(F.col("amount").asc_nulls_first())

result = (
    df_payments
    .select(
        "*",
        F.avg("amount").over(w_recent).alias("amount_avg_30d"),
        F.lag("amount", 1).over(w_ordered).alias("prev_amount"),
        F.rank().over(w3).alias("amount_rank"),
        F.rank().over(w4).alias("amount_rank_2"),
    )
)
