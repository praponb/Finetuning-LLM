from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary", "id")
w2 = Window.partitionBy("job_title", "level").orderBy("hire_date", "id")
w3 = Window.partitionBy("dept_id")

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "email",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.row_number().over(w).alias("event_seq"),
        F.lag("salary", 1).over(w2).alias("prev_salary"),
        F.min("salary").over(w3).alias("dept_id_min_salary"),
    )
)
