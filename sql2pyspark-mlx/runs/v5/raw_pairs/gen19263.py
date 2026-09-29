from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")

cleaned = (
    df_sessions
    .filter(F.col("device").isin(["tablet", "mobile", "desktop"]))
    .select("*", F.coalesce("country", F.lit("Unknown")).alias("country_filled"))
)

result = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
