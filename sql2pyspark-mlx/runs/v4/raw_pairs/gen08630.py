from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("age", "id")
w2 = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("credit_limit", "id")

result = (
    df_customers
    .withColumn("age_bucket", F.ntile(5).over(w))
    .withColumn("cumulative_credit_limit", F.sum("credit_limit").over(w2))
    .withColumn("credit_limit_bucket", F.ntile(4).over(w3))
)
