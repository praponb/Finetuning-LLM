from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("dept_id", "level")
w_part = Window.orderBy("salary", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_employees
    .select(
        "*",
        F.avg("salary").over(w).alias("salary_avg_3"),
        F.min("salary").over(w2).alias("dept_id_min_salary"),
        F.avg("salary").over(w_part).alias("salary_avg_6"),
    )
)
