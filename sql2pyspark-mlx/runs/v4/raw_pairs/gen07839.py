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
        F.first("browser").over(w).alias("first_browser"),
    )
)
