from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device", "country").orderBy(F.col("started_at").desc(), "id")
w_hist = Window.partitionBy("browser", "customer_id").orderBy("started_at", "id")
w_recent = Window.partitionBy("customer_id").orderBy("started_at", "id")

result = (
    df_sessions
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("event_seq_2", F.row_number().over(w_hist))
    .withColumn("seq_no", F.row_number().over(w_recent))
)
