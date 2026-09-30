from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at")
w2 = Window.orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .filter(F.col("device") == "desktop")
    .select(
        "id",
        "device",
        "started_at",
        F.dense_rank().over(w).alias("started_at_dense_rank"),
        F.rank().over(w2).alias("started_at_rank"),
    )
)
