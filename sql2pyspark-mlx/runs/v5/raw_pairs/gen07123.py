from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("budget", "id").rowsBetween(-5, -1)

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-06-15").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "level",
        F.substring("level", 1, 3).alias("level_prefix"),
    )
)

joined = (
    cleaned
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.dept_name") == "HR")
    .select("src.*", F.col("d.budget"), F.col("d.dept_name"))
)

filtered = (
    joined
    .filter(~F.col("dept_name").isin(["Engineering", "HR", "Marketing"]))
    .select(
        "id",
        "full_name",
        "dept_id",
        "job_title",
        "salary",
        "budget",
        "dept_name",
        F.trim("dept_name").alias("clean_dept_name"),
        F.upper("email").alias("email_upper"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("resolved_job_title"),
    )
)

scored = (
    filtered
    .select(
        "id",
        "job_title",
        "salary",
        "budget",
        "dept_name",
        "clean_dept_name",
        "email_upper",
        "resolved_job_title",
        F.sum("salary").over(w).alias("cumulative_salary"),
        F.avg("salary").over(w2).alias("salary_avg_5_prior"),
    )
)

cleaned_employees = (
    scored
    .filter(F.col("dept_name") == "Legal")
    .select(
        "*",
        F.concat("job_title", F.lit(", "), "dept_name").alias("job_title_dept_name"),
        F.coalesce("dept_name", F.lit("OTHER")).alias("resolved_dept_name"),
    )
)

stats = (
    cleaned_employees
    .groupBy(
        F.when(F.col("salary") >= 149000, "PREMIUM").otherwise("STANDARD").alias("salary_tier"),
    )
    .agg(
        F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_rows"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("dept_name") == "Marketing", F.col("salary_avg_5_prior")).otherwise(0)).alias("marketing_salary_avg_5_prior"),
        F.sum(F.when(F.col("job_title") == "Engineer", F.col("salary")).otherwise(0)).alias("engineer_salary"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.sum(F.when(F.col("job_title") == "Intern", F.col("salary")).otherwise(0)).alias("intern_salary"),
    )
)

result = (
    stats
    .select(
        "salary_tier",
        "sales_rows",
        "row_count",
        "marketing_salary_avg_5_prior",
        "budget_quartiles",
        "intern_salary",
    )
)
