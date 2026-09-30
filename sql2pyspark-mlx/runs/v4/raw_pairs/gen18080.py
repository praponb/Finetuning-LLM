from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("duration_sec").desc())

result = (
    df_web_events
    .select(
        "id",
        "duration_sec",
        F.dense_rank().over(w).alias("duration_sec_dense_rank"),
    )
)
