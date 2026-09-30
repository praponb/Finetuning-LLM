from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id")
w2 = Window.partitionBy("dept_id")

result = (
    df_employees
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "level",
        F.lag("salary", 1).over(w).alias("prev_salary"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
    )
)
