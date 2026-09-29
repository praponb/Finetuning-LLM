from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_type_length").desc(), "id")

valid = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2024-05-17").cast("date"))
    .withColumn("event_type_lower", F.lower("event_type"))
    .withColumn("event_type_length", F.length("event_type"))
    .withColumn("page_clean", F.coalesce("page", F.lit("NONE")))
)

result = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
