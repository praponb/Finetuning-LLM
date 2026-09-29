from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("salary").desc(), "id")

enriched = (
    df_employees
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.dept_name"), F.col("d.budget"), F.col("d.location"))
)

with_windows = (
    enriched
    .withColumn("running_salary", F.sum("salary").over(w))
    .withColumn("salary_bucket", F.ntile(5).over(w2))
)

valid = (
    with_windows
    .filter((F.col("hire_date") >= F.lit("2024-02-23").cast("date")) & F.col("location").isNotNull())
    .withColumn("budget_times_salary", (F.col("budget") * F.col("salary")))
)

cleaned = (
    valid
    .filter((F.col("hire_date") >= F.lit("2023-10-31").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "*",
        F.when(F.col("running_salary") >= 606531, "High").when(F.col("running_salary") >= 120418, "Medium").otherwise("Low").alias("running_salary_tier"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
    )
    .orderBy(F.col("hire_date").desc_nulls_last())
)
