from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .filter(F.to_date("started_at") >= "2023-03-01")
    .select("id", "country", "started_at", F.row_number().over(w).alias("rn"))
)
