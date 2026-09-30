from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("device", "customer_id").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("customer_id", "country").orderBy("started_at", "id")
w4 = Window.partitionBy("country", "browser").orderBy(F.col("started_at").desc(), "id")
w5 = Window.partitionBy("browser", "device").orderBy("started_at", "id")

result = (
    df_sessions
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("seq_no_2", F.row_number().over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("row_seq_2", F.row_number().over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
)
