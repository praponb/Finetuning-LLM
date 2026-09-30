from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("status", "order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("amount")

result = (
    df_payments
    .filter(F.col("method") == "cash")
    .select(
        "*",
        F.sum("amount").over(w).alias("status_sum_amount"),
        F.sum("amount").over(w2).alias("running_amount"),
        F.rank().over(w3).alias("amount_rank"),
    )
)
