from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.lead("salary").over(w).alias("next_salary"),
        F.lag("salary").over(w).alias("prev_salary"),
        F.row_number().over(w).alias("event_seq"),
    )
)
