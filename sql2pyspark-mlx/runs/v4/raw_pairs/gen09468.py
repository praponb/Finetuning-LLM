from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_first())

result = (
    df_departments
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)
