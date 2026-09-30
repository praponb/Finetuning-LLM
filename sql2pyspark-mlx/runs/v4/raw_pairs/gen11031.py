from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

valid = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("budget_int", F.col("budget").cast("int"))
    .withColumn("location_clean", F.coalesce("location", F.lit("NONE")))
)

ranked = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

latest_match = (
    ranked
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level").alias("employee_level"))
)

daily = (
    latest_match
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3061000, "LARGE").otherwise("SMALL").alias("budget_tier"),
        "location",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum("budget_int") / F.nullif(F.count(F.when(F.col("dept_name") == "HR", 1)), F.lit(0)), 2).alias("budget_int_per_hr"),
        F.percentile_approx("budget_int", [0.25, 0.5, 0.75]).alias("budget_int_quartiles"),
    )
)

result = (
    daily
    .select(
        "dept_name",
        "budget_tier",
        "location",
        "budget_int_per_hr",
        "budget_int_quartiles",
        F.when(F.col("dept_name").rlike("^(Engineering|HR)$"), "GROUP_A").when(F.col("dept_name").rlike("^(Sales|Marketing)$"), "CORE").otherwise("PRIORITY").alias("dept_name_zone"),
    )
)
