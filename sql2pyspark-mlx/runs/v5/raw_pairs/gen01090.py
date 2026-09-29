from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device", "country").orderBy(F.col("started_at").asc_nulls_first(), "id")

ranked = (
    df_sessions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .select("id", "started_at", "country")
    .orderBy("started_at", "country")
)
