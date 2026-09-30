from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .filter(F.lower(F.trim("email")) == "frank@company.co")
    .select(
        "id",
        "full_name",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.count("salary").over(w).alias("running_count_salary"),
        F.min("salary").over(w).alias("running_min_salary"),
        F.avg("salary").over(w).alias("running_avg_salary"),
        F.sum("salary").over(w).alias("running_sum_salary"),
    )
)
