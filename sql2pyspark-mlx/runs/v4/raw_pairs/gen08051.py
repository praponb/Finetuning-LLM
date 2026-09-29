from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .filter(F.col("shipping_fee") > 11)
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 1)
    .select("id", "customer_id", "order_date", "channel")
)
