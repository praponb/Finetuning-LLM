from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("location").orderBy(F.col("latest_rank").desc(), "id")

deduped = (
    df_departments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

ranked = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

cleaned = (
    ranked
    .filter(F.col("location").isNotNull())
    .select("*", F.lower("dept_name").alias("dept_name_lower"))
)

result = (
    cleaned
    .select("id", "location", "budget", "latest_rank", "rank_in_group")
    .orderBy("rank_in_group")
)
