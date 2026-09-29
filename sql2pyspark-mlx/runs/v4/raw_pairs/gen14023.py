from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at")

result = (
    df_sessions
    .select(
        "id",
        "country",
        "started_at",
        "browser",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 2)
    .select("id", "country", "started_at", "browser")
    .orderBy(F.col("id").desc())
)
