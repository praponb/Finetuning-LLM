from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at")

result = (
    df_sessions
    .filter(F.hour("started_at").between(12, 18))
    .select(
        "id",
        "browser",
        "started_at",
        F.dense_rank().over(w).alias("started_at_dense_rank"),
    )
    .orderBy(F.col("browser").desc())
)
