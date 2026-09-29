from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("location").orderBy(F.col("employee_hire_date").desc_nulls_last(), "id")

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull() & (~F.col("location").eqNullSafe(F.lit("Tokyo"))))
    .select(
        "*",
        F.when(F.col("dept_name").isin(["Marketing", "Engineering"]), "Priority").otherwise("Other").alias("dept_name_group"),
        F.round((F.col("budget") * 0.9), 2).alias("budget_adjusted"),
        F.coalesce("dept_name", F.lit("N/A")).alias("dept_name_clean"),
    )
)

base = (
    cleaned
    .filter(F.col("location").isNotNull() & (F.col("location") == "New York"))
    .select(
        "id",
        "dept_name",
        "location",
        "dept_name_group",
        "budget_adjusted",
        "dept_name_clean",
        F.trim("location").alias("clean_location"),
        F.coalesce("location", F.lit("UNKNOWN")).alias("location_clean"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location",
        "s.dept_name_group",
        "s.location_clean",
        F.col("c.salary"),
        F.col("c.hire_date").alias("employee_hire_date"),
    )
)

deduped = (
    with_last
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "dept_name_group",
        "location_clean",
        "salary",
        "employee_hire_date",
        "latest_rank",
    )
)
