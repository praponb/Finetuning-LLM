from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc(), "id")

result = (
    df_payments
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") == 1)
    .select("id", "order_id", "paid_at")
    .orderBy(F.col("order_id").desc(), "paid_at")
)
