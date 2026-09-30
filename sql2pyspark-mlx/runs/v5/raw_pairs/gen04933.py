from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget")

latest = (
    df_departments
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select("id", "dept_name", "budget", "row_num")
    .orderBy(F.col("row_num").desc())
)
