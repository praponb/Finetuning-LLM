from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id")

result = (
    df_payments
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "order_id", "amount")
    .orderBy(F.col("id").desc())
)
