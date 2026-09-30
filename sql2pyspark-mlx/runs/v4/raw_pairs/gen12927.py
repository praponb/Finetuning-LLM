from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title")
w2 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .filter(F.col("job_title") == "Designer")
    .select(
        "id",
        "job_title",
        "hire_date",
        "salary",
        F.count("*").over(w).alias("job_title_count"),
        F.sum("salary").over(w2).alias("running_salary"),
    )
    .orderBy(F.col("running_salary").desc())
)
