from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_orders
    .withColumn("avg_discount_by_status", F.avg("discount").over(w))
    .filter(F.col("discount") < F.col("avg_discount_by_status"))
    .select("id", "status", "discount", "amount")
)
