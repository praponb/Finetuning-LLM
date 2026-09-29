from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level").orderBy("salary", "id")

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "manager_id",
        "salary",
        "hire_date",
        F.sum("salary").over(w).alias("running_salary"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
