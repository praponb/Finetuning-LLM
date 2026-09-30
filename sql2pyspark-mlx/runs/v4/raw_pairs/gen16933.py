from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_orders
    .withColumn("min_discount_by_status", F.min("discount").over(w))
    .filter(F.col("discount") == F.col("min_discount_by_status"))
    .select("id", "status", "discount", "customer_id")
)
