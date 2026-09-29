from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("level", "manager_id")
w4 = Window.partitionBy("job_title").orderBy("level_max_salary", "id")
w5 = Window.partitionBy("manager_id").orderBy("salary", "id")

enriched = (
    df_employees
    .withColumn("first_known_job_title", F.first("job_title", ignorenulls=True).over(w_recent))
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w2)), 4))
    .withColumn("level_max_salary", F.max("salary").over(w3))
)

enriched_employees = (
    enriched
    .filter(F.col("salary_share") <= 0.59)
    .select(
        "*",
        F.sum("salary").over(w3).alias("level_sum_salary"),
        F.row_number().over(w4).alias("event_seq"),
        F.lead("salary_share", 1, 0).over(w5).alias("next_salary_share"),
    )
)

joined = (
    enriched_employees
    .alias("a")
    .join(df_departments.alias("b"), F.col("a.dept_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.full_name",
        "a.dept_id",
        "a.manager_id",
        "a.job_title",
        "a.salary",
        "a.hire_date",
        "a.first_known_job_title",
        "a.salary_share",
        "a.level_max_salary",
        "a.event_seq",
        "a.next_salary_share",
        F.col("b.dept_name"),
        F.col("b.budget"),
        F.col("b.location"),
    )
)

base = (
    joined
    .filter(F.col("location").isNotNull() & (F.col("next_salary_share") > 0.36))
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "first_known_job_title",
        "salary_share",
        "level_max_salary",
        "event_seq",
        "next_salary_share",
        "dept_name",
        "location",
        F.round((F.col("level_max_salary") * 1.15), 2).alias("adjusted_level_max_salary"),
        F.greatest("salary_share", "salary").alias("greatest_salary_share_salary"),
    )
)

result = (
    base
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "first_known_job_title",
        "level_max_salary",
        "event_seq",
        "next_salary_share",
        "dept_name",
    )
)
