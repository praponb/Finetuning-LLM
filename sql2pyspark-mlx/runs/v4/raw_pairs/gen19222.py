from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("manager_id", "job_title")

metrics = (
    df_employees
    .select(
        "*",
        F.first("job_title", ignorenulls=True).over(w_recent).alias("first_known_job_title"),
        F.max("salary").over(w2).alias("manager_id_max_salary"),
    )
)

result = (
    metrics
    .select("id", "full_name", "email", "job_title", "salary", "is_remote", "level")
    .orderBy("salary")
)
