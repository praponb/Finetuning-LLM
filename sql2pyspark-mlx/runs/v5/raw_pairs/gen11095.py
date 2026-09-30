from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .filter(F.col("browser") != "Firefox")
    .select(
        "id",
        "device",
        "started_at",
        F.dense_rank().over(w).alias("started_at_dense_rank"),
    )
)
