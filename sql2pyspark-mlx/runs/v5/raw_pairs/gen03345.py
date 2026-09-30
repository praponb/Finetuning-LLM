from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "manager_id")
w2 = Window.partitionBy("dept_id", "level").orderBy("hire_date", "id").rowsBetween(-3, -1)
w3 = Window.orderBy("hire_date", "id")
w4 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(-5, -1)
w5 = Window.orderBy("employee_hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("level", "job_title")
w7 = Window.partitionBy("job_title").orderBy("employee_salary_share", "id").rowsBetween(-2, -1)
w8 = Window.partitionBy("manager_id")

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-03-28").cast("date")) & F.col("job_title").isNotNull())
    .select("*", F.upper("job_title").alias("job_title_upper"))
)

enriched = (
    cleaned
    .alias("e")
    .join(df_employees.alias("b"), F.col("e.manager_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.salary").alias("employee_salary"),
        F.col("b.full_name").alias("employee_full_name"),
        F.col("b.hire_date").alias("employee_hire_date"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.round((F.col("employee_salary") / F.sum("employee_salary").over(w)), 4).alias("employee_salary_share"),
        F.avg("salary").over(w2).alias("salary_avg_3_prior"),
        F.row_number().over(w3).alias("row_seq"),
        F.avg("salary").over(w4).alias("salary_avg_5_prior"),
        F.last("job_title", ignorenulls=True).over(w5).alias("last_known_job_title"),
    )
)

scored_employees = (
    scored
    .filter(F.col("hire_date") > "2023-07-17")
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
        "job_title_upper",
        "employee_salary",
        "employee_hire_date",
        "employee_salary_share",
        "row_seq",
        F.stddev_samp("row_seq").over(w6).alias("level_stddev_samp_row_seq"),
        F.avg("salary_avg_3_prior").over(w7).alias("salary_avg_3_prior_avg_2_prior"),
        F.sum("salary_avg_5_prior").over(w8).alias("manager_id_sum_salary_avg_5_prior"),
    )
)

result = (
    scored_employees
    .select(
        "id",
        "full_name",
        "email",
        "salary",
        "hire_date",
        "level",
        "job_title_upper",
        "employee_salary",
        "employee_salary_share",
        F.when(F.col("level").rlike("^(L5|L1)$"), "EXTENDED").when(F.col("level").rlike("^(L2|L3)$"), "GROUP_A").otherwise("CORE").alias("level_zone"),
    )
)
