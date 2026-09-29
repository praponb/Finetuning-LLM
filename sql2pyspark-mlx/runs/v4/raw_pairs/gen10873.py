from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .select("id", "browser", "started_at", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 1)
    .orderBy("started_at", "browser")
)
