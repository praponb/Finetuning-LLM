from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc_nulls_first(), "id")

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select("*", F.trim("dept_name").alias("clean_dept_name"))
)

result = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "dept_name", "budget", "clean_dept_name")
)
