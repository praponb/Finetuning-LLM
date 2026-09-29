from pyspark.sql import functions as F

matched_keys = (
    df_departments
    .select("dept_name", "id")
    .intersect(
        df_departments
        .filter(F.col("location") != "Berlin")
        .select("dept_name", "id")
    )
)

reconciled = (
    df_departments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.when(F.col("dept_name").rlike("^(HR|Finance)$"), "GROUP_A").when(F.col("dept_name").rlike("^(Engineering|Sales)$"), "PRIORITY").otherwise("CORE").alias("dept_name_zone"),
    )
    .orderBy("dept_name")
)
