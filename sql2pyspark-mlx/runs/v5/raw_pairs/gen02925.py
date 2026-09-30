from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(-6, Window.currentRow)

with_windows = (
    df_employees
    .select(
        "id",
        "email",
        "manager_id",
        "salary",
        F.max("salary").over(w_running).alias("last7_max_salary"),
        F.sum("salary").over(w_running).alias("last7_sum_salary"),
        F.avg("salary").over(w_running).alias("last7_avg_salary"),
        F.min("salary").over(w_running).alias("last7_min_salary"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "email",
        "manager_id",
        "last7_max_salary",
        "last7_sum_salary",
        "last7_min_salary",
    )
)
