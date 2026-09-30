from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "browser").orderBy(F.col("started_at").desc(), "id")
w_recent = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w_recent))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w_recent))
)
