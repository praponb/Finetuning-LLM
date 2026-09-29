from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("dept_id", "level").orderBy("hire_date", "id")
w_running = Window.orderBy(F.col("salary").desc())

with_windows = (
    df_employees
    .select(
        "id",
        "full_name",
        "salary",
        "is_remote",
        F.lead("salary").over(w_ordered).alias("next_salary"),
        F.lag("salary").over(w_ordered).alias("prev_salary"),
        F.row_number().over(w_ordered).alias("event_seq"),
        F.percent_rank().over(w_running).alias("salary_percent_rank"),
    )
)

result = (
    with_windows
    .select("id", "is_remote", "prev_salary", "event_seq")
    .orderBy(F.col("prev_salary").desc_nulls_last())
)
