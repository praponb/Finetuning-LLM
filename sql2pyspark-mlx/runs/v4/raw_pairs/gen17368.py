from pyspark.sql import functions as F

kept = (
    df_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.salary") < 124000), "left_semi")
)

result = (
    kept
    .select("id", "dept_name", "budget")
    .orderBy(F.col("id").desc_nulls_last())
)
