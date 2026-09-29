from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("amount")
w_recent = Window.orderBy("amount", "id")
w_hist = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_payments
    .withColumn("amount_dense_rank", F.dense_rank().over(w_ordered))
    .withColumn("seq_no", F.row_number().over(w_recent))
    .withColumn("last_known_method", F.last("method", ignorenulls=True).over(w_hist))
)
