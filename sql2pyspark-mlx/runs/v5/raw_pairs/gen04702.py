from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")
w_running_2 = Window.partitionBy("manager_id").orderBy("salary", "id")
w5 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "dept_id",
        "job_title",
        "salary",
        "level",
        F.sum("salary").over(w).alias("running_salary"),
        F.sum("salary").over(w2).alias("cumulative_salary"),
        F.row_number().over(w_running).alias("seq_no"),
        F.ntile(4).over(w_running_2).alias("salary_bucket"),
        F.first("job_title", ignorenulls=True).over(w5).alias("first_known_job_title"),
    )
)
