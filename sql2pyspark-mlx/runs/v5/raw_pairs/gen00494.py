from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .filter(F.hour("paid_at").between(8, 15))
    .select("id", "order_id", "amount", F.ntile(2).over(w).alias("quartile"))
)
