from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("amount", "id")
w2 = Window.partitionBy("order_id", "method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status", "method").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .filter(F.col("status") == "FAILED")
    .select(
        "id",
        "order_id",
        "method",
        "status",
        F.lead("amount", 2).over(w_hist).alias("next_amount"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.sum("amount").over(w2).alias("cumulative_amount"),
        F.ntile(10).over(w3).alias("amount_bucket"),
    )
)
