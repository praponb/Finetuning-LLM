from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "channel").orderBy("amount", "id")
w2 = Window.partitionBy("customer_id", "status").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_orders
    .withColumn("amount_bucket", F.ntile(3).over(w))
    .withColumn("last_known_discount", F.last("discount", ignorenulls=True).over(w2))
)
