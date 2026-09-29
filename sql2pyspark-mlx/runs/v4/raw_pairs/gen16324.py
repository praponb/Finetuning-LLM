from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("paid_at", "id")

result = (
    df_payments
    .withColumn("first_known_method", F.first("method", ignorenulls=True).over(w))
    .withColumn("amount_count_7d", F.count("amount").over(w2))
    .withColumn("prev_amount", F.lag("amount", 1, 0).over(w3))
)
