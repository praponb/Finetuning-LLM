from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id", "browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("browser", "device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter(F.col("country") == "TH")
    .withColumn("country_browser_key", F.concat_ws("-", "country", "browser"))
)

result = (
    valid
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w_running))
    .withColumn("last_known_browser_2", F.last("browser", ignorenulls=True).over(w2))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w3))
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w4))
)
