from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.coalesce("dept_name", F.lit("unassigned")).alias("dept_name_filled"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("dept_name_clean"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.job_title") == "Intern"), "left_semi")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title"))
)

cleaned = (
    latest_match
    .filter(F.col("job_title").isNotNull())
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "dept_name_clean",
        F.trim("dept_name").alias("clean_dept_name"),
    )
)

result = (
    cleaned
    .select("id", "dept_name", "location", "budget", "dept_name_clean")
    .orderBy(F.col("id").desc())
)
