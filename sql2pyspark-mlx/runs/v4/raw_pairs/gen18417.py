from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("browser").isNotNull())
    .select(
        "id",
        "browser",
        "started_at",
        "device",
        F.lag("started_at", 1).over(w).alias("prev_started_at"),
        F.row_number().over(w2).alias("rn"),
    )
    .orderBy("prev_started_at")
)
