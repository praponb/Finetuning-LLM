from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "dept_id").orderBy("hire_date", "id")
w2 = Window.orderBy("hire_date", "id").rowsBetween(-5, -1)

result = (
    df_employees
    .select(
        "*",
        F.lag("salary", 1).over(w).alias("prev_salary"),
        F.avg("salary").over(w2).alias("salary_avg_5_prior"),
    )
)
