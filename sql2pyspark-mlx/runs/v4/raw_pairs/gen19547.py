from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("amount").asc_nulls_last())
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_payments
    .withColumn("last_known_method", F.last("method", ignorenulls=True).over(w))
    .withColumn("amount_dense_rank", F.dense_rank().over(w2))
    .withColumn("running_amount", F.sum("amount").over(w3))
)

result = (
    scored
    .select("id", "method", "amount", "paid_at", "last_known_method", "running_amount")
    .orderBy(F.col("paid_at").asc_nulls_last())
)
