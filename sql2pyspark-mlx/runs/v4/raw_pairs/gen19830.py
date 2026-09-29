from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.partitionBy("job_title")

result = (
    df_employees
    .select(
        "id",
        "job_title",
        "hire_date",
        "salary",
        F.ntile(10).over(w).alias("hire_date_ntile"),
        F.sum("salary").over(w2).alias("job_title_sum_salary"),
    )
)
