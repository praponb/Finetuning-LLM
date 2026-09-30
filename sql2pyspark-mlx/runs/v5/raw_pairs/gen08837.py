from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id")

result = (
    df_employees
    .filter(F.col("dept_id").isin([8, 28, 36]))
    .select("id", "job_title", "salary", F.row_number().over(w).alias("salary_row"))
    .orderBy("job_title")
)
