from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc_nulls_last(), "id")

deduped = (
    df_payments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select("id", "method", "paid_at")
    .orderBy(F.col("id").desc_nulls_first())
)
