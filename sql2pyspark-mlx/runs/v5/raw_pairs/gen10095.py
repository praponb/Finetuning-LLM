from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")
w2 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_employees
    .filter(F.col("job_title") == "Analyst")
    .select(
        "id",
        "manager_id",
        "salary",
        "level",
        "hire_date",
        F.min("salary").over(w).alias("manager_id_min_salary"),
        F.round(F.avg("salary").over(w2), 2).alias("salary_moving_avg_7"),
    )
)
