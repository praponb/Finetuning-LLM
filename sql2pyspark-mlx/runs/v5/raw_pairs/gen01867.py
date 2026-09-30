from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")
w3 = Window.partitionBy("manager_id").orderBy("hire_date", "id")
w4 = Window.partitionBy("manager_id", "dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(-3, Window.currentRow)

with_windows = (
    df_employees
    .select(
        "id",
        "dept_id",
        "level",
        F.sum("salary").over(w).alias("cumulative_salary"),
        F.row_number().over(w_part).alias("event_seq"),
        F.row_number().over(w3).alias("seq_no"),
        F.last("level", ignorenulls=True).over(w4).alias("last_known_level"),
        F.avg("salary").over(w5).alias("salary_avg_3"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "dept_id",
        "level",
        "cumulative_salary",
        "event_seq",
        "seq_no",
        "last_known_level",
        "salary_avg_3",
    )
)
