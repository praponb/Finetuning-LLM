from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_payments
    .select(
        "*",
        F.sum("amount").over(w).alias("running_amount"),
        F.last("amount", ignorenulls=True).over(w2).alias("last_known_amount"),
    )
)
