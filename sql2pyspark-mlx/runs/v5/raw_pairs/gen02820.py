from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id")
w2 = Window.orderBy(F.col("amount").desc())

result = (
    df_transactions
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("amount_percent_rank", F.percent_rank().over(w2))
)
