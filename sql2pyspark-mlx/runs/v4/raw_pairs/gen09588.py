from pyspark.sql import functions as F

kept_keys = (
    df_departments
    .select("location")
    .intersectAll(
        df_departments
        .filter(F.col("dept_name") == "Legal")
        .select("location")
    )
)

kept_rows = (
    df_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "dept_name", "budget")
    .orderBy(F.col("budget").desc_nulls_first())
)
