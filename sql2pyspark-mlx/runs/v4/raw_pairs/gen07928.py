from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("started_at", "id")

result = (
    df_sessions
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)
