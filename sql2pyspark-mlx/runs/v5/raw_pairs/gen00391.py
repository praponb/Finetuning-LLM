from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id")
w_hist = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .select("*", F.col("is_remote").cast("int").alias("is_remote_flag"))
)

result = (
    cleaned
    .select(
        "id",
        "email",
        "job_title",
        "salary",
        "is_remote",
        "is_remote_flag",
        F.lag("salary", 1).over(w).alias("prev_salary"),
        F.sum("salary").over(w_hist).alias("running_salary"),
    )
)
