from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(-2, -1)
w2 = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .select(
        "*",
        F.avg("salary").over(w).alias("salary_avg_2_prior"),
        F.sum("salary").over(w2).alias("cumulative_salary"),
    )
)
