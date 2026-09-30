from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-06-15").cast("date"))
    .withColumn("device_browser", F.concat("device", F.lit(" - "), "browser"))
    .withColumn("browser_lower", F.lower("browser"))
)

result = (
    base
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w))
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w2))
)
