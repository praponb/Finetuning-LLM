from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")

result = (
    df_sessions
    .select("id", "browser", "started_at", F.row_number().over(w).alias("rn"))
)
