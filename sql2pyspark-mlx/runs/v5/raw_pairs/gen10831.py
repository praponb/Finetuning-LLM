from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .select(
        "id",
        "device",
        "started_at",
        "country",
        F.ntile(4).over(w).alias("started_at_ntile"),
        F.first("device").over(w2).alias("first_device"),
    )
)
