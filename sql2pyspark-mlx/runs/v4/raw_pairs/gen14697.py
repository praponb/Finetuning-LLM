from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .filter(F.hour("paid_at").between(7, 11))
    .select("id", "order_id", "amount", F.row_number().over(w).alias("row_num"))
)
