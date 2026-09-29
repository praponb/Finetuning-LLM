from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .filter(F.col("full_name").like("%Kim%"))
    .select(
        "id",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        F.sum("salary").over(w).alias("running_sum_salary"),
        F.max("salary").over(w).alias("running_max_salary"),
        F.min("salary").over(w).alias("running_min_salary"),
    )
)
