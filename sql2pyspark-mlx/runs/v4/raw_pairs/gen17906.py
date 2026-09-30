from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("manager_id").isin([10, 12, 14]))
    .select(
        "id",
        "job_title",
        "hire_date",
        "salary",
        F.lead("salary", 1).over(w).alias("next_salary"),
    )
    .orderBy(F.col("id").desc())
)
