from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w_grp))
    .withColumn("event_seq", F.row_number().over(w_hist))
    .withColumn("event_seq_2", F.row_number().over(w_hist))
)
