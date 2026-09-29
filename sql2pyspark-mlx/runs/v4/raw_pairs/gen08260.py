from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .filter(F.col("discount") >= 0.39)
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 5)
    .select("id", "customer_id", "amount", "order_date")
)
