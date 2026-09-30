from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("started_at").desc(), "id")
w_grp = Window.partitionBy("device", "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w_hist = Window.partitionBy("country", "browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w_grp))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w_hist))
)
