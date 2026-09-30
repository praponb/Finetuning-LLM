from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select("*", F.initcap(F.trim("dept_name")).alias("dept_name_title"))
)

result = (
    cleaned
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.manager_id").isNotNull()), "left_semi")
)
