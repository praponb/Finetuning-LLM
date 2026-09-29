from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("method").orderBy("amount", "id")

result = (
    df_payments
    .filter(F.hour("paid_at").between(9, 15))
    .withColumn("prev_amount", F.lag("amount").over(w_hist))
    .withColumn("event_seq", F.row_number().over(w_hist))
)
