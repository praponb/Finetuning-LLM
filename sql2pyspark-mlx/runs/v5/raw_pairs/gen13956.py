from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device", "customer_id").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("started_at") < "2025-07-01")
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)
