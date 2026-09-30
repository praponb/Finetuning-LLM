from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "manager_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("manager_id")

result = (
    df_employees
    .filter(F.col("job_title") == "Analyst")
    .select(
        "id",
        "dept_id",
        "manager_id",
        "hire_date",
        "salary",
        F.ntile(2).over(w).alias("hire_date_ntile"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 3).alias("salary_share"),
    )
    .orderBy("hire_date")
)
