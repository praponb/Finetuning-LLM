from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").asc_nulls_last(), "id")

deduped = (
    df_sessions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .withColumn("week_start", F.date_trunc("WEEK", "started_at"))
    .groupingSets(
        [["week_start", "device"], ["week_start"], []],
        "week_start",
        "device",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_count"),
        F.min("started_at").alias("first_started_at"),
        F.countDistinct("browser").alias("distinct_browsers"),
    )
)
