from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title")
w2 = Window.partitionBy("level").orderBy(F.col("hire_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_employees
    .select(
        "id",
        "job_title",
        "salary",
        "level",
        "hire_date",
        "full_name",
        F.sum("salary").over(w).alias("job_title_sum_salary"),
        F.last("full_name").over(w2).alias("last_full_name"),
    )
)
