from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")

base = (
    df_departments
    .filter(F.col("dept_name") != "Legal")
    .withColumn("location_length", F.length("location"))
    .withColumn("resolved_dept_name", F.coalesce("dept_name", F.lit("NONE")))
)

result = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "dept_name", "location", "location_length")
)
