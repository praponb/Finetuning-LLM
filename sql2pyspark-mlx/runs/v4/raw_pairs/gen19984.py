from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("started_at", "id")

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-08-18 10:00:00").cast("timestamp")) & F.col("country").isNotNull())
    .withColumn("country_device", F.concat("country", F.lit(" - "), "device"))
)

result = (
    filtered
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w))
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w_hist))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("first_known_country_2", F.first("country", ignorenulls=True).over(w_hist))
)
