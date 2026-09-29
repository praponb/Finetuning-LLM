from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("country").orderBy("started_at", "id")
w_part = Window.partitionBy("browser", "customer_id").orderBy(F.col("started_at").desc(), "id")
w_grp = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .withColumn("seq_no", F.row_number().over(w_running))
    .withColumn("seq_no_2", F.row_number().over(w_part))
    .withColumn("event_seq", F.row_number().over(w_part))
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w_grp))
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w_grp))
)
