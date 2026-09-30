from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method")
w2 = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("method", "status").orderBy("amount", "id")
w5 = Window.partitionBy("order_id", "method").orderBy("amount")

result = (
    df_payments
    .select(
        "*",
        F.sum("amount").over(w).alias("method_sum_amount"),
        F.count("amount").over(w2).alias("amount_count_30d"),
        F.first("amount", ignorenulls=True).over(w3).alias("first_known_amount"),
        F.row_number().over(w4).alias("seq_no"),
        F.rank().over(w5).alias("amount_rank"),
    )
)
