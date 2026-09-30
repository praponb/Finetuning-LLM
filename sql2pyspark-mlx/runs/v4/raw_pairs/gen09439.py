from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc())

result = (
    df_employees
    .select("id", "dept_id", "hire_date", "job_title", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 2)
    .select("id", "dept_id", "hire_date", "job_title")
)
