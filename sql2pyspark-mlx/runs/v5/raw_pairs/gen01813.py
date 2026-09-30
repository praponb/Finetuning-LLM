from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "level").orderBy("level_length_minus_salary", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-05-16").cast("date"))
    .withColumn("level_length", F.length("level"))
    .withColumn("level_clean", F.coalesce("level", F.lit("OTHER")))
)

joined = (
    base
    .alias("s")
    .join(df_departments.alias("p"), F.col("s.dept_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.full_name",
        "s.dept_id",
        "s.manager_id",
        "s.salary",
        "s.level",
        "s.level_length",
        F.col("p.budget"),
    )
)

base_employees = (
    joined
    .filter(F.col("level").isNotNull() & (~F.col("level").eqNullSafe(F.lit("L4"))))
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "level",
        "level_length",
        (F.col("level_length") - F.col("salary")).alias("level_length_minus_salary"),
        F.round((F.col("salary") * 1.07), 2).alias("salary_adjusted"),
        F.coalesce("level", F.lit("DIRECT")).alias("resolved_level"),
    )
)

metrics = (
    base_employees
    .withColumn("last4_avg_level_length_minus_salary", F.avg("level_length_minus_salary").over(w))
    .withColumn("last4_min_level_length_minus_salary", F.min("level_length_minus_salary").over(w))
    .withColumn("last4_count_level_length_minus_salary", F.count("level_length_minus_salary").over(w))
)

result = (
    metrics
    .select(
        "id",
        "dept_id",
        "last4_min_level_length_minus_salary",
        F.col("salary_adjusted").alias("SALARY_ADJUSTED"),
    )
    .unpivot(
        ids=["id", "dept_id"],
        values=["last4_min_level_length_minus_salary", "SALARY_ADJUSTED"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
