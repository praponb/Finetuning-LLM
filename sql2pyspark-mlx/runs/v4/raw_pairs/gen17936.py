from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("started_at", "id")
w_grp = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w_running))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w_grp))
)
