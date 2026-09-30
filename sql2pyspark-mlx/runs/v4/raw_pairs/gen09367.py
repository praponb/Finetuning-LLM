from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("dept_id", "manager_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("job_title")
w_recent = Window.partitionBy("dept_id").orderBy("salary", "id")
w_hist = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .select(
        "*",
        F.lead("salary", 1).over(w_running).alias("next_salary"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.first("level", ignorenulls=True).over(w_hist).alias("first_known_level"),
    )
)
