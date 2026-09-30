from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy(F.col("amount").asc_nulls_last())

result = (
    df_payments
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("amount_rank", F.rank().over(w2))
)
