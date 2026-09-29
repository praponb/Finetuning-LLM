from pyspark.sql import functions as F

kept_keys = (
    df_departments
    .select("dept_name", "location")
    .subtract(
        df_departments
        .filter(F.col("budget") <= 3307000)
        .select("dept_name", "location")
    )
)

kept_rows = (
    df_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "location",
        F.when(F.col("location").rlike("^(New York|London)$"), "GROUP_A").otherwise("CORE").alias("location_zone"),
    )
    .orderBy(F.col("location_zone").desc_nulls_first(), "id")
)
