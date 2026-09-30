from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")
w2 = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level")

base = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-06-30").cast("date")) & F.col("dept_id").isin([16, 20, 21, 33]))
    .select(
        "id",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.upper("level").alias("level_upper"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("level_filled"),
    )
)

result = (
    base
    .select(
        "*",
        F.min("salary").over(w).alias("dept_id_min_salary"),
        F.last("job_title", ignorenulls=True).over(w2).alias("last_known_job_title"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share"),
    )
)
