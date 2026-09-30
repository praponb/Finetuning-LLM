from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("dept_id").orderBy("hire_date", "id")

cleaned = (
    df_employees
    .filter(F.col("manager_id").isNotNull() & F.col("full_name").like("G%"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.upper(F.trim("full_name")).alias("full_name_normalized"),
        F.coalesce("level", F.lit("N/A")).alias("level_filled"),
    )
)

result = (
    cleaned
    .filter(F.col("dept_id") == 27)
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "is_remote",
        "full_name_normalized",
        F.sum("salary").over(w_part).alias("cumulative_salary"),
        F.avg("salary").over(w2).alias("salary_avg_6_prior"),
        F.lead("salary", 2).over(w3).alias("next_salary"),
    )
)
