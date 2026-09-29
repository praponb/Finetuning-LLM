from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc_nulls_last())

cleaned = (
    df_sessions
    .filter(F.col("country").isNotNull() & (~F.col("country").eqNullSafe(F.lit("US"))))
    .select("*", F.concat_ws("-", "browser", "device").alias("browser_device_key"))
)

result = (
    cleaned
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)
