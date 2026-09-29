from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id").rowsBetween(-5, -1)
w2 = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .select(
        "id",
        "job_title",
        "salary",
        "hire_date",
        F.avg("salary").over(w).alias("salary_avg_5_prior"),
        F.sum("salary").over(w2).alias("running_salary"),
    )
)
