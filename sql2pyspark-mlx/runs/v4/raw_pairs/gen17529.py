from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("dept_name_length", F.length("dept_name"))
)

result = (
    valid
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)
