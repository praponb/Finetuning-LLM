from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc())

deduped = (
    df_departments
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select("id", "location", "budget")
    .orderBy(F.col("location").asc_nulls_first(), "budget")
)
