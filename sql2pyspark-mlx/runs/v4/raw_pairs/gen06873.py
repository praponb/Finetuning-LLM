from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id", "browser").orderBy("started_at", "id")

result = (
    df_sessions
    .withColumn("event_seq", F.row_number().over(w_part))
    .withColumn("row_seq", F.row_number().over(w2))
)
