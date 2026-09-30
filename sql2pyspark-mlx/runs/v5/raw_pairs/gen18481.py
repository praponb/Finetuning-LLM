from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec")

result = (
    df_web_events
    .filter(F.col("session_id").isin([13, 16]))
    .select(
        "id",
        "page",
        "duration_sec",
        F.dense_rank().over(w).alias("duration_sec_dense_rank"),
    )
)
