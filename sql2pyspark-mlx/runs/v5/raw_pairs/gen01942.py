from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("dept_id").orderBy("salary", "id")
w_hist = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc(), "id")

result = (
    df_employees
    .filter(F.month("hire_date").isin([5, 6, 12]))
    .select(
        "id",
        "email",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.lead("salary").over(w_ordered).alias("next_salary"),
        F.lag("salary").over(w_ordered).alias("prev_salary"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)
