from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("level_length", "id")
w2 = Window.partitionBy("manager_id")
w_running = Window.partitionBy("job_title", "manager_id").orderBy("level_length", "id")

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-03-15").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
        F.coalesce("job_title", F.lit("NONE")).alias("job_title_clean"),
    )
)

enriched = (
    valid
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.email").alias("employee_email"),
        F.col("p.salary").alias("employee_salary"),
    )
)

base = (
    enriched
    .filter(F.col("hire_date") > F.lit("2023-05-02").cast("date"))
    .select(
        "*",
        F.length("level").alias("level_length"),
        F.upper("employee_email").alias("employee_email_upper"),
        F.coalesce("job_title", F.lit("OTHER")).alias("job_title_clean_2"),
    )
)

result = (
    base
    .select(
        "*",
        F.ntile(3).over(w).alias("level_length_bucket"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
        F.lag("salary", 1).over(w_running).alias("prev_salary"),
    )
)
