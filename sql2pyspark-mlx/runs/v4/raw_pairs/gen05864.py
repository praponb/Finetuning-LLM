from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_first())

latest = (
    df_departments
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    latest
    .select("id", "dept_name", "location", "budget")
    .orderBy(F.col("location").desc_nulls_first(), F.col("budget").desc())
)
