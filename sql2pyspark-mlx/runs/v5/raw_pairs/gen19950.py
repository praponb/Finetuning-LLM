from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device", "browser").orderBy(F.col("started_at").desc(), "id")

with_windows = (
    df_sessions
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w))
)

result = (
    with_windows
    .select("id", "customer_id", "device", "country", "first_known_country")
    .orderBy(F.col("device").desc_nulls_first())
)
