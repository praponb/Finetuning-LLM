from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .filter(F.hour("started_at").between(11, 17))
    .select(
        "id",
        "country",
        "started_at",
        "device",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 5)
    .orderBy("country")
)
