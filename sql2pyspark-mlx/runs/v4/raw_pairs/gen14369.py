from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at")
w2 = Window.partitionBy("device").orderBy("started_at")

result = (
    df_sessions
    .select(
        "id",
        "browser",
        "started_at",
        "device",
        F.dense_rank().over(w).alias("started_at_dense_rank"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy(F.col("id").desc())
)
