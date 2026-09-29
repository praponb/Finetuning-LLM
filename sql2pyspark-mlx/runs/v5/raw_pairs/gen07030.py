from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "dept_id").orderBy("salary", "id")

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "dept_id",
        "salary",
        "hire_date",
        "is_remote",
        F.lag("salary").over(w).alias("prev_salary"),
        F.row_number().over(w).alias("event_seq"),
    )
)
