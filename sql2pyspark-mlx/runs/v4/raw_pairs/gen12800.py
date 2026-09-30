from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec", "id")

result = (
    df_web_events
    .filter(F.col("page").isin(["/search", "/checkout", "/product"]))
    .select("id", "event_type", "duration_sec", F.row_number().over(w).alias("rn"))
)
