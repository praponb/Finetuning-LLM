from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")

deduped = (
    df_sessions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .groupBy("browser")
    .agg(
        F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_count"),
        F.min("started_at").alias("started_at"),
        F.count("*").alias("row_count"),
    )
    .orderBy("row_count")
)
