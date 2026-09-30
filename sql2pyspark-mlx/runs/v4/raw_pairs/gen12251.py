from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "job_title")
w2 = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-06-24").cast("date")) & F.col("manager_id").isNotNull() & F.col("job_title").isin(["Manager", "Engineer", "Director"]))
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        F.date_sub("hire_date", 29).alias("hire_date_minus_29d"),
    )
)

result = (
    valid
    .select(
        "*",
        F.min("salary").over(w).alias("manager_id_min_salary"),
        F.first("job_title", ignorenulls=True).over(w2).alias("first_known_job_title"),
    )
)
