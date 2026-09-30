from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .select("id", "browser", "started_at", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "browser", "started_at")
    .orderBy(F.col("id").desc())
)
