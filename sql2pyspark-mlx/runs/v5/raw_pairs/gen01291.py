from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_employees
    .filter(F.col("job_title").isin(["Manager", "Analyst", "Director"]))
    .select(
        "id",
        "manager_id",
        "salary",
        "hire_date",
        F.last("hire_date").over(w).alias("last_hire_date"),
    )
)
