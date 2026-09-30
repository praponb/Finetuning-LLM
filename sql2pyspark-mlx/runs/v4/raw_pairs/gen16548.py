from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "job_title").orderBy("salary", "id")
w2 = Window.partitionBy("job_title", "level").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "*",
        F.ntile(3).over(w).alias("salary_bucket"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
