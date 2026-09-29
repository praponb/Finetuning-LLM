from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "dept_id").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "level",
        "dept_id",
        "hire_date",
        "salary",
        F.sum("salary").over(w).alias("running_salary"),
    )
)
