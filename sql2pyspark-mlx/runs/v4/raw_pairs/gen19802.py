from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id")

result = (
    df_employees
    .select("id", "job_title", "hire_date", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .select("id", "job_title", "hire_date")
)
