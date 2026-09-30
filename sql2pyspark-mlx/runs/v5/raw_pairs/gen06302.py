from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("location").eqNullSafe(F.lit("Singapore")))
    .withColumn("dept_name_group", F.when(F.col("dept_name").isin(["HR", "Finance"]), "Priority").otherwise("Other"))
)

valid_departments = (
    valid
    .filter(F.col("dept_name") == "Finance")
    .withColumn("dept_name_title", F.initcap(F.trim("dept_name")))
)

filtered = (
    valid_departments
    .filter(F.col("dept_name").isNotNull() & (~F.col("dept_name").eqNullSafe(F.lit("HR"))))
    .select(
        "id",
        "dept_name",
        "budget",
        "dept_name_group",
        F.col("budget").cast("int").alias("budget_int"),
        F.coalesce("location", F.lit("UNKNOWN")).alias("resolved_location"),
    )
)

kept = (
    filtered
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.manager_id").isin([24, 28, 36])), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.full_name").isNull()), "left_anti")
)

result = not_flagged.select("id", "dept_name", "budget", "budget_int")
