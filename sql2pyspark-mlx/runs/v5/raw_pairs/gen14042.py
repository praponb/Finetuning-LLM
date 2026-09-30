from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy(F.col("paid_at").desc(), "id")
w2 = Window.partitionBy("status", "method").orderBy("amount", "id")
w3 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

result = (
    df_payments
    .select(
        "id",
        "method",
        F.row_number().over(w_hist).alias("row_seq"),
        F.lead("amount", 1).over(w2).alias("next_amount"),
        F.sum("amount").over(w3).alias("running_amount"),
        F.count("amount").over(w4).alias("amount_count_30d"),
    )
)
