from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc())
w2 = Window.orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_payments
    .select(
        "*",
        F.percent_rank().over(w).alias("amount_percent_rank"),
        F.ntile(3).over(w2).alias("amount_bucket"),
        F.first("amount", ignorenulls=True).over(w3).alias("first_known_amount"),
    )
)
