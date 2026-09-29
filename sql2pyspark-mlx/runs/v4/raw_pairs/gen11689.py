from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("browser").orderBy("started_at", "id")

with_windows = (
    df_sessions
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
)

result = (
    with_windows
    .select("id", "device", "browser", "country", "last_known_browser", "row_seq")
)
