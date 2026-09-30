from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .filter(F.col("country").isNull())
    .select("id", "browser", "started_at", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "browser", "started_at")
)
