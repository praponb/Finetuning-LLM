from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_employees
    .filter(F.col("full_name").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "level",
        F.min("salary").over(w).alias("last4_min_salary"),
        F.avg("salary").over(w).alias("last4_avg_salary"),
        F.max("salary").over(w).alias("last4_max_salary"),
        F.sum("salary").over(w).alias("last4_sum_salary"),
    )
)
