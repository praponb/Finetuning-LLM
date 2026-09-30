from pyspark.sql import functions as F

kept_keys = (
    df_departments
    .select("id", "dept_name")
    .intersect(
        df_departments
        .filter(F.col("dept_name") == "Legal")
        .select("id", "dept_name")
    )
)

verified = (
    df_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "dept_name",
        "budget",
        F.when(F.col("dept_name").rlike("^(Legal|HR)$"), "EXTENDED").when(F.col("dept_name").rlike("^(Engineering|Sales)$"), "GROUP_B").otherwise("GROUP_A").alias("dept_name_zone"),
    )
    .orderBy("id")
)
