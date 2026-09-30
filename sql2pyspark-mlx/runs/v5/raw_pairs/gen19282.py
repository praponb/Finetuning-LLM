from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "email",
        "job_title",
        "salary",
        "level",
        F.sum("salary").over(w).alias("running_salary"),
        F.last("level", ignorenulls=True).over(w2).alias("last_known_level"),
        F.ntile(10).over(w3).alias("salary_bucket"),
    )
)
