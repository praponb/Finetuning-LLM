from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "device").orderBy("started_at", "id")

result = (
    df_sessions
    .select(
        "id",
        "browser",
        "device",
        "started_at",
        F.lead("started_at", 1).over(w).alias("next_started_at"),
    )
)
