from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("employee_level").orderBy("budget_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_name").orderBy(F.col("budget_int").desc(), "id")
w_part = Window.partitionBy("employee_level").orderBy("budget", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

cleaned = (
    df_departments
    .filter(F.col("location") == "Tokyo")
    .select(
        "*",
        F.col("budget").cast("int").alias("budget_int"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("resolved_dept_name"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date"))
)

latest_match_departments = (
    latest_match
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level").alias("employee_level"))
)

metrics = (
    latest_match_departments
    .withColumn("first_known_budget_int", F.first("budget_int", ignorenulls=True).over(w2))
    .withColumn("budget_int_bucket", F.ntile(10).over(w3))
    .withColumn("budget_bucket", F.ntile(3).over(w_part))
)

with_last = (
    metrics
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title"))
)

result = (
    with_last
    .select(
        "id",
        "dept_name",
        "location",
        "budget_int",
        "resolved_dept_name",
        "employee_level",
        "budget_bucket",
        "job_title",
        F.when(F.col("dept_name").rlike("^(HR|Legal)$"), "GROUP_B").when(F.col("dept_name").rlike("^(Engineering|Sales)$"), "GROUP_A").otherwise("EXTENDED").alias("dept_name_zone"),
    )
    .orderBy(F.col("budget_int").desc_nulls_last())
)
