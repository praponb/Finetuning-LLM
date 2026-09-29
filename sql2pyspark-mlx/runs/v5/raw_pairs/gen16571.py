from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .filter(F.col("budget") > 3894000)
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 1)
    .select("id", "dept_name", "budget", "location")
)
