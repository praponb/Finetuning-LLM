from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 2)
    .select("id", "location", "budget", "dept_name")
    .orderBy(F.col("dept_name").desc(), "location")
)
