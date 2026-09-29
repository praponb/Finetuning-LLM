from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level", "dept_id").orderBy("salary", "id")

result = (
    df_employees
    .select(
        "id",
        "email",
        "job_title",
        F.max("salary").over(w).alias("running_max_salary"),
        F.sum("salary").over(w).alias("running_sum_salary"),
        F.avg("salary").over(w).alias("running_avg_salary"),
        F.ntile(5).over(w2).alias("salary_bucket"),
    )
)
