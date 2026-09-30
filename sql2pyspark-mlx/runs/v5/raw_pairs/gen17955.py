from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "job_title",
        "hire_date",
        "salary",
        F.lag("salary", 2).over(w).alias("prev_salary"),
    )
    .orderBy(F.col("hire_date").desc())
)
