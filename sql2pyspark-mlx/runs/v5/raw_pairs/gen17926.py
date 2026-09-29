from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at")

result = (
    df_sessions
    .filter(F.col("country") == "JP")
    .select(
        "id",
        "country",
        "started_at",
        "device",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .select("id", "country", "started_at", "device")
    .orderBy(F.col("id").desc(), F.col("started_at").desc())
)
