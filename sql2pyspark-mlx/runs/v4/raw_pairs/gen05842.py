from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .select(
        "id",
        "country",
        "started_at",
        "browser",
        F.dense_rank().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 1)
    .select("id", "country", "started_at", "browser")
)
