from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_payments
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("cumulative_amount", F.sum("amount").over(w2))
    .withColumn("cumulative_amount_2", F.sum("amount").over(w3))
)
