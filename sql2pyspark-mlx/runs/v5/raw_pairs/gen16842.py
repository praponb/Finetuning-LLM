from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")

ranked = (
    df_sessions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "country")
)

result = (
    ranked
    .select("id")
    .orderBy(F.col("id").desc())
)
