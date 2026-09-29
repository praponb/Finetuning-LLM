from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id")
w2 = Window.partitionBy("dept_id").orderBy("hire_date")

result = (
    df_employees
    .filter(F.col("salary").between(61000, 142000))
    .select(
        "id",
        "dept_id",
        "salary",
        "job_title",
        "hire_date",
        F.first("job_title").over(w).alias("first_job_title"),
        F.percent_rank().over(w2).alias("hire_date_pct_rank"),
    )
)
