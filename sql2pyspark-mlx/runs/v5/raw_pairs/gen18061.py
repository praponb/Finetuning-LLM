from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("days_since_hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_name").orderBy(F.col("salary").desc(), "id")
w3 = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc(), "id")
w4 = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_id").orderBy("days_since_hire_date", "id")
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.sum("salary").alias("sum_employee_salary"))

base = (
    df_employees
    .filter(F.col("dept_id").isin([7, 27, 32, 38]))
    .select(
        "*",
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
        F.coalesce("level", F.lit("DIRECT")).alias("resolved_level"),
    )
)

with_dims = (
    base
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("d.location") != "London")
    .select("src.*", F.col("d.dept_name"), F.col("d.location"))
)

enriched = (
    with_dims
    .withColumn("first_known_dept_name", F.first("dept_name", ignorenulls=True).over(w))
    .withColumn("salary_bucket", F.ntile(10).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("first_known_dept_name_2", F.first("dept_name", ignorenulls=True).over(w4))
    .withColumn("next_salary", F.lead("salary", 1).over(w5))
)

cleaned = (
    enriched
    .filter(F.col("manager_id").isNotNull())
    .withColumn("job_title_length", F.length("job_title"))
    .withColumn("level_filled", F.coalesce("level", F.lit("UNKNOWN")))
)

cleaned_employees = (
    cleaned
    .filter(F.col("salary_bucket") >= 4)
    .select(
        "*",
        (F.col("next_salary") * F.col("row_seq")).alias("next_salary_times_row_seq"),
    )
)

result = (
    cleaned_employees
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "days_since_hire_date",
        F.when(F.col("level").rlike("^(L4|L2)$"), "GROUP_B").otherwise("GROUP_A").alias("level_zone"),
        F.col("sum_employee_salary"),
    )
    .orderBy(F.col("manager_id").asc_nulls_last())
)
