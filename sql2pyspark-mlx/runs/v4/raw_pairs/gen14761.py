from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2024-01-25").cast("date")) & F.col("job_title").isNotNull() & F.col("full_name").like("Ali%"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        F.trim("level").alias("clean_level"),
    )
)

scored = (
    valid
    .withColumn("first_known_job_title", F.first("job_title", ignorenulls=True).over(w))
    .withColumn("last_known_job_title", F.last("job_title", ignorenulls=True).over(w2))
    .withColumn("last_known_job_title_2", F.last("job_title", ignorenulls=True).over(w3))
)

joined = (
    scored
    .alias("s")
    .join(df_employees.alias("e"), F.col("s.manager_id") == F.col("e.id"), "left")
    .select(
        "s.*",
        F.col("e.email").alias("employee_email"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.level"),
    )
)

result = (
    joined
    .select(
        "id",
        "dept_id",
        "job_title",
        "hire_date",
        "is_remote",
        "last_known_job_title_2",
        "employee_email",
        "employee_full_name",
        "level",
        F.when(F.col("level").rlike("^(L1|L2)$"), "GROUP_B").when(F.col("level").rlike("^(L3|L4)$"), "EXTENDED").otherwise("CORE").alias("level_zone"),
    )
    .orderBy(F.col("job_title").desc_nulls_first())
)
