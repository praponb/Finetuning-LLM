from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id")

result = (
    df_employees
    .select("id", "dept_id", "salary", "job_title", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "dept_id", "salary", "job_title")
    .orderBy(F.col("dept_id").desc())
)
