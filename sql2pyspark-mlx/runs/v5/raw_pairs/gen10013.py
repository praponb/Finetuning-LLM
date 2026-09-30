from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("job_title")

result = (
    df_employees
    .filter(F.upper("email").like("%MMA%"))
    .select(
        "id",
        "manager_id",
        "hire_date",
        "salary",
        "job_title",
        F.round(F.avg("salary").over(w), 2).alias("salary_moving_avg_4"),
        F.max("salary").over(w2).alias("job_title_max_salary"),
    )
    .orderBy(F.col("id").desc())
)
