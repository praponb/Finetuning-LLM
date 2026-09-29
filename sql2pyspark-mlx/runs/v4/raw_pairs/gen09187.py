from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc(), "id")

result = (
    df_payments
    .filter(F.col("order_id").isin([12, 18, 27]))
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 5)
    .select("id", "order_id", "paid_at", "method")
    .orderBy(F.col("order_id").desc())
)
