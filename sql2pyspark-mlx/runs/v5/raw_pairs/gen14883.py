from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")

result = (
    df_sessions
    .select(
        "id",
        "device",
        "started_at",
        "browser",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .orderBy(F.col("started_at").desc(), F.col("row_num").desc())
)
