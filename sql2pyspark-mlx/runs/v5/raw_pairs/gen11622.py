from pyspark.sql import functions as F

remaining_keys = (
    df_departments
    .select("dept_name")
    .intersectAll(
        df_departments
        .filter(F.col("budget") < 2164000)
        .select("dept_name")
    )
)

kept_rows = (
    df_departments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("dept_name", "budget")
    .orderBy(F.col("budget").desc_nulls_first())
)
