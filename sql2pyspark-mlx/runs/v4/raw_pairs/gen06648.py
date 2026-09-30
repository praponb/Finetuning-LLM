from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("days_since_reading_ts", "id")

base = (
    df_device_readings
    .filter(F.col("reading_ts") < "2025-05-10")
    .withColumn("days_since_reading_ts", F.datediff(F.current_date(), "reading_ts"))
)

result = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)
