from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .filter(F.col("country").isNull())
    .select("id", "country", "started_at", "device", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .select("id", "country", "started_at", "device")
)
