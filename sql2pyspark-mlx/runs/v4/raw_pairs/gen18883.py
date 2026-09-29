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
        (F.col("salary") - F.lag("salary", 1).over(w)).alias("salary_change"),
    )
    .orderBy("job_title")
)
