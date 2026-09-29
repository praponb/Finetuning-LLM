from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("device") == "mobile")
    .select(
        "id",
        "country",
        "started_at",
        "device",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "country", "started_at", "device")
)
